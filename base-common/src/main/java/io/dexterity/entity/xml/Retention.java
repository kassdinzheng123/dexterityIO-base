package io.dexterity.entity;

/**
 * <pre>
 *  Retention
 * </pre>
 * @author kassdin
 * @verison $Id: Retention v 0.1 2023-03-19 17:54:59
 */
public class Retention{

    /**
     * <pre>
     *
     * </pre>
     */
    private String	Mode;

    /**
     * <pre>
     *
     * </pre>
     */
    private String	RetainUntilDate;


    public String getMode() {
        return this.Mode;
    }

    public void setMode(String Mode) {
        this.Mode = Mode;
    }

    public String getRetainUntilDate() {
        return this.RetainUntilDate;
    }

    public void setRetainUntilDate(String RetainUntilDate) {
        this.RetainUntilDate = RetainUntilDate;
    }

}
