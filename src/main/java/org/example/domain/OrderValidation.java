package org.example.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderValidation {

    private String orderId;
    private OrderValidationType checkType;
    private OrderValidationResult validationResult;

    public enum OrderValidationType {
        INVENTORY_CHECK, FRAUD_CHECK, ORDER_DETAILS_CHECK
    }

    public enum OrderValidationResult {
        PASS, FAIL, ERROR
    }

}
