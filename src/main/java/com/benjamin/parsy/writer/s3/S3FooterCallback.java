package com.benjamin.parsy.writer.s3;

/**
 * Interface de rappel pour écrire un pied de page dans un fichier envoyé en multipart sur S3
 */
public interface S3FooterCallback {

    /**
     * Permet d'écrire le pied de page du fichier
     */
    byte[] writeFooter();

}
