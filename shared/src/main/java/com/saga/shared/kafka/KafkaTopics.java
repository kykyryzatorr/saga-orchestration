package com.saga.shared.kafka;

// Topic name convention: {domain}.{event}
public final class KafkaTopics {

    private KafkaTopics() {}

    // ─── Order ───────────────────────────────────────────────────────────────
    public static final String ORDER_CREATE_REQUEST = "order.create.request";
    public static final String ORDER_CREATED   = "order.created";
    public static final String ORDER_CONFIRMED = "order.confirmed";
    public static final String ORDER_CANCELLED = "order.cancelled";

    // ─── Stock ───────────────────────────────────────────────────────────────
    public static final String STOCK_RESERVE_REQUEST    = "stock.reserve.request";
    public static final String STOCK_RESERVED           = "stock.reserved";
    public static final String STOCK_RESERVATION_FAILED = "stock.reservation.failed";
    public static final String STOCK_RELEASE_REQUEST    = "stock.release.request";
    public static final String STOCK_RELEASED           = "stock.released";

    // ─── Payment ─────────────────────────────────────────────────────────────
    public static final String PAYMENT_PROCESS_REQUEST = "payment.process.request";
    public static final String PAYMENT_PROCESSED       = "payment.processed";
    public static final String PAYMENT_FAILED          = "payment.failed";
    public static final String PAYMENT_CANCEL_REQUEST  = "payment.cancel.request";
    public static final String PAYMENT_CANCELLED       = "payment.cancelled";
}
