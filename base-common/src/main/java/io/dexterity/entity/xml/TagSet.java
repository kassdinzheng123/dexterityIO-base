package io.dexterity.entity.xml;

import java.util.List;

/**
 * <pre>
 *  TagSet
 * </pre>
 * @author kassdin
 * @verison $Id: TagSet v 0.1 2023-03-19 17:57:23
 */
public class TagSet{

    /**
     * <pre>
     * Tag
     * </pre>
     */
    private List<Tag>	Tag;


    public List<Tag> getTag() {
        return this.Tag;
    }

    public void setTag(List<Tag> Tag) {
        this.Tag = Tag;
    }

}
