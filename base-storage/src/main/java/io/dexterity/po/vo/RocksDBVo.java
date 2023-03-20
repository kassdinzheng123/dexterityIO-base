package io.dexterity.po.vo;

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
    private byte[] key;

    private byte[] value;
}