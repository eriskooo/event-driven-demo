package cz.demo.eda.order.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/** Objednávky v PostgreSQL (schéma orders). */
@Transactional(readOnly = true)
public interface OrderRepository extends JpaRepository<Order, String> {

    /** Načte objednávku se zámkem řádku – souběžné výsledky plateb se tak zpracují postupně. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select o from Order o where o.id = :id")
    Optional<Order> findForUpdate(String id);
}
