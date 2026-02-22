package com.benjamin.parsy.writer.s3;

public interface S3HeaderCallback {

    /**
     * Permet d'écrire l'en-tête du fichier
     */
    byte[] writeHeader();

}
