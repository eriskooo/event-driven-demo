package cz.demo.eda.payment.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/** Platby v PostgreSQL (schéma payments). */
@Transactional(readOnly = true)
public interface PaymentRepository extends JpaRepository<Payment, String> {

    /** Najde platbu podle ID objednávky. */
    Optional<Payment> findByOrderId(String orderId);
}
