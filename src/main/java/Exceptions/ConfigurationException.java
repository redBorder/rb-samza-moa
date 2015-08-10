package Exceptions;

/**
 * Created by Chorro on 04/08/15.
 */
public class ConfigurationException extends Exception {
    public ConfigurationException(String mensaje){
        super(mensaje);
        // Podemos añadir otras funcionalidades para la excepción de configuración
    }
}
