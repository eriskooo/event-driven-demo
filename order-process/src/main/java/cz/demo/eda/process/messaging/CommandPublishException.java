package cz.demo.eda.process.messaging;

/** Příkaz se nepodařilo předat Kafce; job zůstane nedokončený a Zeebe ho zopakuje. */
public class CommandPublishException extends RuntimeException {

    /** Vytvoří výjimku pro topic, do kterého se nepodařilo zapsat. */
    public CommandPublishException(String topic, Throwable cause) {
        super("Failed to publish command to " + topic, cause);
    }
}
