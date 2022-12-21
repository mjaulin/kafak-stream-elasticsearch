package org.example.payments;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.avro.Payment;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentBean {

    private String id;
    private String orderId;
    private String ccy;
    private double amount;

    public Payment toPayment() {
        return new Payment(id, orderId, ccy, amount);
    }

}
