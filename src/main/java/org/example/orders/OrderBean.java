package org.example.orders;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.avro.Order;
import org.example.avro.OrderState;
import org.example.avro.Product;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderBean {

    private String id;
    private int customerId;
    private OrderState state;
    private Product product;
    private int quantity;
    private double price;

    public OrderBean(final Order order) {
        this.id = order.getId();
        this.customerId = order.getCustomerId();
        this.state = order.getState();
        this.product = order.getProduct();
        this.quantity = order.getQuantity();
        this.price = order.getPrice();
    }

    public Order toOrder() {
        return new Order(id, customerId, state, product, quantity, price);
    }

}
