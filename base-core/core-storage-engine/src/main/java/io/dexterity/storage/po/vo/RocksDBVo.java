package io.dexterity.storage.po.vo;

import lombok.*;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RocksDBVo implements Serializable {
    @NonNull
    @Builder.Default
    private String cfName = "default";
    @NonNull
    private String key;
    private String value;
}