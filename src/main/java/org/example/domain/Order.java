package org.example.domain;

import lombok.Data;

@Data
public class Order {

    private String id;
    private Long customerId;
    private OrderState state;
    private Product product;
    private Integer quantity;
    private Double price;

    public enum OrderState {
        CREATED, VALIDATED, FAILED, SHIPPED
    }

}
