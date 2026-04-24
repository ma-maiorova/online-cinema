package com.cinema.producer.kafka;

import com.cinema.avro.DeviceType;
import com.cinema.avro.EventType;
import com.cinema.producer.error.AppError;
import com.cinema.producer.error.ErrorCode;
import com.cinema.producer.error.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for EventValidator — parses string values into Avro enums.
 */
@DisplayName("Event Validator Tests")
class EventValidatorTest {

    private EventValidator eventValidator;

    @BeforeEach
    void setUp() {
        eventValidator = new EventValidator();
    }

    // ─── EventType parsing ────────────────────────────────────────────────────

    @ParameterizedTest(name = "parseEventType({0}) should succeed")
    @EnumSource(EventType.class)
    @DisplayName("All valid EventType values parse successfully")
    void shouldParseAllValidEventTypes(EventType eventType) {
        // When
        Result<EventType, AppError> result = eventValidator.parseEventType(eventType.name());

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(eventType);
    }

    @Test
    @DisplayName("VIEW_STARTED parses to correct enum value")
    void shouldParseViewStarted() {
        Result<EventType, AppError> result = eventValidator.parseEventType("VIEW_STARTED");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(EventType.VIEW_STARTED);
    }

    @Test
    @DisplayName("VIEW_FINISHED parses to correct enum value")
    void shouldParseViewFinished() {
        Result<EventType, AppError> result = eventValidator.parseEventType("VIEW_FINISHED");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(EventType.VIEW_FINISHED);
    }

    @Test
    @DisplayName("LIKED parses to correct enum value")
    void shouldParseLiked() {
        Result<EventType, AppError> result = eventValidator.parseEventType("LIKED");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(EventType.LIKED);
    }

    @Test
    @DisplayName("SEARCHED parses to correct enum value")
    void shouldParseSearched() {
        Result<EventType, AppError> result = eventValidator.parseEventType("SEARCHED");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(EventType.SEARCHED);
    }

    @ParameterizedTest(name = "parseEventType(''{0}'') should fail with INVALID_EVENT_TYPE")
    @ValueSource(strings = {"INVALID", "view_started", "VIEW", "", "PLAY", "STOP", "unknown"})
    @DisplayName("Invalid EventType values return INVALID_EVENT_TYPE error")
    void shouldReturnErrorForInvalidEventType(String invalidValue) {
        // When
        Result<EventType, AppError> result = eventValidator.parseEventType(invalidValue);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.INVALID_EVENT_TYPE);
        assertThat(result.getError().message()).contains(invalidValue);
        assertThat(result.getError().details()).containsKey("received");
        assertThat(result.getError().details()).containsKey("allowed");
        assertThat(result.getError().details().get("received")).isEqualTo(invalidValue);
    }

    // ─── DeviceType parsing ───────────────────────────────────────────────────

    @ParameterizedTest(name = "parseDeviceType({0}) should succeed")
    @EnumSource(DeviceType.class)
    @DisplayName("All valid DeviceType values parse successfully")
    void shouldParseAllValidDeviceTypes(DeviceType deviceType) {
        // When
        Result<DeviceType, AppError> result = eventValidator.parseDeviceType(deviceType.name());

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(deviceType);
    }

    @Test
    @DisplayName("MOBILE parses to correct enum value")
    void shouldParseMobile() {
        Result<DeviceType, AppError> result = eventValidator.parseDeviceType("MOBILE");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(DeviceType.MOBILE);
    }

    @Test
    @DisplayName("DESKTOP parses to correct enum value")
    void shouldParseDesktop() {
        Result<DeviceType, AppError> result = eventValidator.parseDeviceType("DESKTOP");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(DeviceType.DESKTOP);
    }

    @Test
    @DisplayName("TV parses to correct enum value")
    void shouldParseTv() {
        Result<DeviceType, AppError> result = eventValidator.parseDeviceType("TV");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(DeviceType.TV);
    }

    @Test
    @DisplayName("TABLET parses to correct enum value")
    void shouldParseTablet() {
        Result<DeviceType, AppError> result = eventValidator.parseDeviceType("TABLET");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(DeviceType.TABLET);
    }

    @ParameterizedTest(name = "parseDeviceType(''{0}'') should fail with INVALID_DEVICE_TYPE")
    @ValueSource(strings = {"PHONE", "mobile", "SMART_TV", "", "PC", "LAPTOP"})
    @DisplayName("Invalid DeviceType values return INVALID_DEVICE_TYPE error")
    void shouldReturnErrorForInvalidDeviceType(String invalidValue) {
        // When
        Result<DeviceType, AppError> result = eventValidator.parseDeviceType(invalidValue);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.INVALID_DEVICE_TYPE);
        assertThat(result.getError().message()).contains(invalidValue);
        assertThat(result.getError().details()).containsKey("received");
        assertThat(result.getError().details().get("received")).isEqualTo(invalidValue);
    }
}
