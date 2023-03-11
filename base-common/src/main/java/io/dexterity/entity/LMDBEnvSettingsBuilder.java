package io.dexterity.entity;

public class LMDBEnvSettingsBuilder {
    private final LMDBEnvSettings lmdbEnvSettings;

    private LMDBEnvSettingsBuilder(LMDBEnvSettings lmdbEnvSettings){
        this.lmdbEnvSettings = lmdbEnvSettings;
    }
    public static LMDBEnvSettingsBuilder startBuild(){
        return new LMDBEnvSettingsBuilder(new LMDBEnvSettings());
    }

    public LMDBEnvSettingsBuilder maxSize(int maxSize){
        this.lmdbEnvSettings.setMaxSize(maxSize);
        return this;
    }

    public LMDBEnvSettingsBuilder maxDBInstance(int maxDBInstance){
        this.lmdbEnvSettings.setMaxDBInstance(maxDBInstance);
        return this;
    }

    public LMDBEnvSettingsBuilder maxReaders(int maxReaders){
        this.lmdbEnvSettings.setMaxReaders(maxReaders);
        return this;
    }

    public LMDBEnvSettingsBuilder filePosition(String filePosition){
        this.lmdbEnvSettings.setFilePosition(filePosition);
        return this;
    }

    public LMDBEnvSettingsBuilder envName(String envName){
        this.lmdbEnvSettings.setEnvName(envName);
        return this;
    }

    public LMDBEnvSettingsBuilder secretKey(String secretKey){
        this.lmdbEnvSettings.setSecretKey(secretKey);
        return this;
    }

    public LMDBEnvSettings build(){
        return lmdbEnvSettings;
    }
}
