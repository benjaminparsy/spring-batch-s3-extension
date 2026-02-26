package com.benjamin.parsy.spring.batch.s3.extension.writer;

/**
 * Interface de rappel pour écrire un pied de page dans un fichier envoyé en multipart sur S3
 */
public interface S3FooterCallback {

    /**
     * Permet d'écrire le pied de page du fichier
     */
    byte[] writeFooter();

}
