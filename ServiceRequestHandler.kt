package service.stp.reservation.web.service

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import commons.stp.reservation.service.AvailabilityService
import commons.stp.reservation.service.STPTariffService
import commons.stp.reservation.service.STReservationService
import commons.stp.reservation.service.exceptions.NoActiveSTPTariffForLocationAndAreaException
import helpers.api.exception.LocationNotFoundException
import helpers.api.exception.PathParameterRequiredException
import helpers.api.ok
import helpers.objectmapping.objectMapper
import integrations.configuration.ConfigurationServiceClient
import integrations.configuration.dto.type.ProjectTypeDto
import integrations.kiosk.client.KioskEventsSQSClient
import integrations.kiosk.message.ServiceShortTermReservationCreatedEventDto
import integrations.parkingspotlock.client.ParkingSpotLockServiceClient
import integrations.parkingspotlock.dto.ServiceParkingSpotLockDto
import integrations.parkingspotlock.dto.ServiceParkingSpotLockForSectorCreationDto
import integrations.stp.reservation.client.dto.ServiceCreateShortTermReservationDto
import integrations.terminal.client.TerminalStateHandlerSQSClient
import integrations.terminal.dto.ServiceTerminalStateHandlerSTPAvailabilityUpdateMessageDto
import integrations.terminal.dto.ServiceTerminalStateHandlerSTPReservationUpdateMessageDto
import service.stp.reservation.config.AppConfiguration
import service.stp.reservation.config.DependencyManager
import service.stp.reservation.web.client.exception.NoSTPAreaWasFoundException
import service.stp.reservation.web.client.exception.NoSTPLocationWasFoundException
import service.stp.reservation.web.client.exception.NoSTPSectorWasFoundException
import logging.log4j.LogManager
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class ServiceRequestHandler private constructor(
    private val stReservationService: STReservationService,
    private val appConfiguration: AppConfiguration,
    private val terminalStateHandlerSQSClient: TerminalStateHandlerSQSClient,
    private val kioskEventsSQSClient: KioskEventsSQSClient,
    private val configurationServiceClient: ConfigurationServiceClient,
    private val availabilityService: AvailabilityService,
    private val stpTariffService: STPTariffService,
    private val parkingSpotLockServiceClient: ParkingSpotLockServiceClient
) {

    private val lambdaLogger = LogManager.getLogger(ServiceRequestHandler::javaClass.name)

    companion object {
        @Volatile
        private var instance: ServiceRequestHandler? = null

        fun getInstance(dm: DependencyManager, alwaysNew: Boolean = false): ServiceRequestHandler {
            if (instance == null || alwaysNew) {
                synchronized(this) {
                    instance = ServiceRequestHandler(
                        stReservationService = dm.getSTReservationService(),
                        appConfiguration = dm.getAppConfiguration(),
                        terminalStateHandlerSQSClient = dm.getTerminalStateHandlerSQSClient(),
                        kioskEventsSQSClient = dm.getKioskEventsSQSClient(),
                        configurationServiceClient = dm.getConfigurationServiceClient(),
                        availabilityService = dm.getAvailabilityService(),
                        stpTariffService = dm.getSTPTariffService(),
                        parkingSpotLockServiceClient = dm.getParkingSpotLockServiceClient()
                        )
                    return instance!!
                }
            }
            return instance!!
        }
    }
    
    fun createReservation(locationIdParam: String?,
                          createReservationRequest: ServiceCreateShortTermReservationDto): APIGatewayProxyResponseEvent {
        // Validate parameters
        val locationId = (locationIdParam?.toLongOrNull()) ?: throw PathParameterRequiredException("locationId")

        lambdaLogger.info("start handling the create short term reservation api request for license plate: ${createReservationRequest.licensePlate} with payload: $createReservationRequest")

        val allowedToExitUntil = createReservationRequest.startTimestamp.plus(
            appConfiguration.allowedToExitPeriodInMinute, ChronoUnit.MINUTES
        )

        val tariffId = stpTariffService.getActiveTariffForLocationAndArea(locationId, createReservationRequest.areaId)?.uuid
            ?: throw  NoActiveSTPTariffForLocationAndAreaException(locationId, createReservationRequest.areaId)

        val parkingSpotLock : ServiceParkingSpotLockDto? = try{
            lambdaLogger.info("Trying to create Parking spot lock for license plate: ${createReservationRequest.licensePlate}")
            parkingSpotLockServiceClient.createParkingSpotLockForSector(
                locationId = locationId,
                parkingSpotLockDto = ServiceParkingSpotLockForSectorCreationDto(
                    areaId = createReservationRequest.areaId,
                    sectorId = createReservationRequest.sectorId,
                    startTimestamp = createReservationRequest.startTimestamp,
                    endTimestamp = createReservationRequest.endTimestamp
                )
            )

        }
        catch (e : Exception){
            lambdaLogger.error("Unfortunately, we were not be able to find a free spot at " +
                    "sector: ${createReservationRequest.sectorId} to be used for license plate :${createReservationRequest.licensePlate} however, " +
                    "we will proceed with creating a short term reservation")
            null
        }

        if(parkingSpotLock != null){
            lambdaLogger.info("Parking spot lock was created for stp-reservation with id:${parkingSpotLock.id} ")
        }

        val reservationId = stReservationService.createReservationIfNewWithinLast24Hours(
            locationId = locationId,
            areaId = createReservationRequest.areaId,
            terminalId = createReservationRequest.terminalId,
            licensePlate = createReservationRequest.licensePlate,
            startTimestamp = createReservationRequest.startTimestamp,
            allowedToExitUntil = allowedToExitUntil,
            sectorId = createReservationRequest.sectorId,
            tariffId = tariffId,
            spotLockUuid = if(parkingSpotLock != null) UUID.fromString(parkingSpotLock.id) else null,
            spotId = parkingSpotLock?.spotId
        )

        if(reservationId == null) {
            lambdaLogger.warn("There is already an active short-term reservation for licensePlate: ${createReservationRequest.licensePlate}")
            return ok
        }

        sendSTPReservationUpdateToTerminalStateHandler(
            reservationId = reservationId,
            terminalId = createReservationRequest.terminalId,
            licensePlate = createReservationRequest.licensePlate,
            allowedToExitUntil = allowedToExitUntil,
            locationId = locationId,
            timestamp = createReservationRequest.startTimestamp,
            areaId = createReservationRequest.areaId,
            sectorId = createReservationRequest.sectorId
        )
        lambdaLogger.info("Notification event was sent to TerminalStateHandler for license plate: ${createReservationRequest.licensePlate}, reservationId: $reservationId, allowedToExit: $allowedToExitUntil")

        sendShortTermReservationCreatedMessageToKioskService(
            locationId = locationId,
            reservationId = reservationId,
            licensePlate = createReservationRequest.licensePlate,
            startTimestamp = createReservationRequest.startTimestamp,
            tariffId = tariffId
        )
        lambdaLogger.info("Notification was sent to Kiosk service for license plate: ${createReservationRequest.licensePlate} , reservation id: $reservationId")

        val spotAvailability = try {
            calculateAvailableSpotForSTPSector(
                locationId = locationId,
                areaId = createReservationRequest.areaId,
                sectorId = createReservationRequest.sectorId
            )
        }catch (e: Exception){
            lambdaLogger.info("An Error was occurred while calculating the spot availability therefore, spot availability update event was not sent to TerminalStateHandler for location: ${locationId}, area: ${createReservationRequest.areaId} and sector: ${createReservationRequest.sectorId}")
            lambdaLogger.error(e.message)
            return ok.withBody(reservationId.toString())
        }

        terminalStateHandlerSQSClient.sendSTPAvailabilityUpdate(
            ServiceTerminalStateHandlerSTPAvailabilityUpdateMessageDto(
                terminalId = createReservationRequest.terminalId,
                locationId = locationId,
                areaId = createReservationRequest.areaId,
                sectorId = createReservationRequest.sectorId,
                availableSpots = spotAvailability
            )
        )
        lambdaLogger.info("spot availability update event was sent to TerminalStateHandler with value: $spotAvailability for location: ${locationId}, area: ${createReservationRequest.areaId} and sector: ${createReservationRequest.sectorId}")

        return ok.withBody(reservationId.toString())
    }

    private fun calculateAvailableSpotForSTPSector(
        locationId: Long,
        areaId: Long,
        sectorId: Long,
    ): Long {

        lambdaLogger.info("Service - calculating number of available spots for location: $locationId and areaId: $areaId and sector: $sectorId   ...")

        val locationConfiguration = configurationServiceClient.loadProjectConfiguration(locationId)?: throw LocationNotFoundException (locationId = locationId)

        val totalNumberOfSpots = locationConfiguration.parkingSpots
            .count { it.sectorId == sectorId }

        lambdaLogger.info("Total number of parking spots for sector: $sectorId was: $totalNumberOfSpots")

        val numberOfFreeSpots = availabilityService.countAvailableSpots(locationId = locationId,
            areaId = areaId, sectorId = sectorId, totalNumberOfSpots.toLong()
        )

        lambdaLogger.info("Service - the number of available STP spots for location: $locationId and areaId: $areaId and sector: $sectorId is $numberOfFreeSpots")

        return numberOfFreeSpots
    }

    private fun sendShortTermReservationCreatedMessageToKioskService(
        locationId: Long,
        reservationId: Long,
        licensePlate: String,
        startTimestamp: Instant,
        tariffId: UUID
    ) {
        kioskEventsSQSClient.receiveShortTermReservationCreatedEvent(
            ServiceShortTermReservationCreatedEventDto(
                locationId = locationId,
                reservationId = reservationId,
                licensePlate = licensePlate,
                startTimestamp = startTimestamp,
                tariffId = tariffId
            )
        )
    }

    private fun sendSTPReservationUpdateToTerminalStateHandler(
        reservationId: Long,
        terminalId: Long,
        locationId: Long,
        timestamp: Instant,
        licensePlate: String,
        allowedToExitUntil: Instant,
        areaId: Long,
        sectorId:Long
    ) {
        val message = ServiceTerminalStateHandlerSTPReservationUpdateMessageDto(
            locationId = locationId,
            reservationId = reservationId,
            timestamp = timestamp.toEpochMilli(),
            terminalId = terminalId,
            licensePlate = licensePlate,
            allowedToExitUntil = allowedToExitUntil.toEpochMilli(),
            areaId = areaId,
            sectorId = sectorId
        )
        terminalStateHandlerSQSClient.sendSTPReservationUpdate(message)
    }

    fun countLocationAvailableSpots(
        locationIdParam: String?,
        areaIdParam: String?
    ): APIGatewayProxyResponseEvent {

        lambdaLogger.info("Service - getting number of available spots for location $locationIdParam and area $areaIdParam  ...")

        // Validate parameters
        val locationId = (locationIdParam?.toLongOrNull()) ?: throw PathParameterRequiredException("locationId")
        val areaId = (areaIdParam?.toLongOrNull()) ?: throw PathParameterRequiredException("areaId")

        val locationConfiguration = configurationServiceClient.loadProjectConfiguration(locationId)

        lambdaLogger.info("location configuration was loaded: $locationConfiguration")

        if(locationConfiguration?.projectType != ProjectTypeDto.STP && locationConfiguration?.projectType != ProjectTypeDto.SHARED)
            throw NoSTPLocationWasFoundException(locationId = locationIdParam)

        val area = locationConfiguration.areas
            .firstOrNull { it.id == areaId && (it.projectType == ProjectTypeDto.STP || it.projectType == ProjectTypeDto.SHARED)}
            ?: throw NoSTPAreaWasFoundException(areaId = areaIdParam)

        val sectorId = area.sectors
            .firstOrNull { it.projectType == ProjectTypeDto.STP || it.projectType == ProjectTypeDto.SHARED}
            ?.id ?: throw NoSTPSectorWasFoundException(locationId = locationIdParam, areaId = areaIdParam)

        val totalNumberOfSpots = locationConfiguration.parkingSpots
            .count { it.sectorId == sectorId }

        lambdaLogger.info("Total number of parking spots for sector: $sectorId was: $totalNumberOfSpots")

        val numberOfFreeSpots = availabilityService.countAvailableSpots(locationId = locationId,
            areaId = areaId, sectorId = sectorId, totalNumberOfSpots.toLong()
        )

        lambdaLogger.info("Service - the number of available STP spots for location $locationIdParam and area $areaIdParam is $numberOfFreeSpots...")

        return ok.withBody(objectMapper.writeValueAsString(numberOfFreeSpots))
    }

    fun countSTPTariffsUsage(
        locationIdParam: String?,
        areaIdParam: String?,
        tariffUuidParam: String?
    ): APIGatewayProxyResponseEvent {

        lambdaLogger.info("Service - getting number of STPTariff $tariffUuidParam usages for $locationIdParam and area $areaIdParam")

        // Validate parameters
        val locationId = (locationIdParam?.toLongOrNull()) ?: throw PathParameterRequiredException("locationId")
        val areaId = (areaIdParam?.toLongOrNull()) ?: throw PathParameterRequiredException("areaId")
        val tariffUuid = tariffUuidParam?: throw PathParameterRequiredException("tariffUuidParam")

        val locationConfiguration = configurationServiceClient.loadProjectConfiguration(locationId)

        lambdaLogger.info("Location configuration was loaded: $locationConfiguration")

        if(locationConfiguration?.projectType != ProjectTypeDto.STP && locationConfiguration?.projectType != ProjectTypeDto.SHARED)
            throw NoSTPLocationWasFoundException(locationId = locationIdParam)

        val area = locationConfiguration.areas
            .firstOrNull { it.id == areaId && (it.projectType == ProjectTypeDto.STP || it.projectType == ProjectTypeDto.SHARED)}
            ?: throw NoSTPAreaWasFoundException(areaId = areaIdParam)

        val sectorId = area.sectors
            .firstOrNull { it.projectType == ProjectTypeDto.STP || it.projectType == ProjectTypeDto.SHARED}
            ?.id ?: throw NoSTPSectorWasFoundException(locationId = locationIdParam, areaId = areaIdParam)

        val numberOfSTPTariffUsage = stReservationService.countSTPTariffsUsage(locationId = locationId, areaId = areaId, sectorId = sectorId, tariffUuid = tariffUuid)

        lambdaLogger.info("Service - the number of STPTariff $tariffUuid usage for $locationIdParam, area $tariffUuid and sector $sectorId is $numberOfSTPTariffUsage")

        return ok.withBody(objectMapper.writeValueAsString(numberOfSTPTariffUsage))
    }
}
