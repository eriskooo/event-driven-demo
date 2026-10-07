package cz.demo.eda.payment.support.jpa;

import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

/**
 * Základ entit s ID přiřazeným aplikací (UUID, eventId). Bez Persistable by Spring Data save()
 * považoval entitu s vyplněným ID za existující a místo INSERT dělal merge (SELECT + INSERT).
 */
@MappedSuperclass
public abstract class AssignedIdEntity<I> implements Persistable<I> {

    @Transient
    private boolean newEntity = true;

    @Override
    public boolean isNew() {
        return newEntity;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        newEntity = false;
    }
}
