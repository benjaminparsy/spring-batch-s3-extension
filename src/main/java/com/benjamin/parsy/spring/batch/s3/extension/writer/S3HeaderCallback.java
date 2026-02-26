package com.benjamin.parsy.spring.batch.s3.extension.writer;

public interface S3HeaderCallback {

    /**
     * Permet d'écrire l'en-tête du fichier
     */
    byte[] writeHeader();

}
