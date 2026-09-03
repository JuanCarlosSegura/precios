package com.gasolineras.precios.producer;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Component
public class GasolinerasProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    // Endpoint JSON de Open-Meteo para las coordenadas de Sevilla
    private static final String API_URL = "https://api.open-meteo.com/v1/forecast?latitude=37.3891&longitude=-5.9845&current=temperature_2m,relative_humidity_2m,wind_speed_10m";

    // Se ejecuta cada 10 segundos llamando a la API en vivo
    @Scheduled(fixedRate = 10000)
    public void descargarYPublicar() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(API_URL))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                String jsonBody = response.body();
                System.out.println(">>> [API EXTERNA REAL] Datos recibidos con éxito desde Open-Meteo.");
                
                // Enviamos el JSON en vivo directo al topic de Kafka
                kafkaTemplate.send("gasolineras-stream", jsonBody);
            } else {
                System.err.println("Error en la API externa. Código: " + response.statusCode());
            }
        } catch (Exception e) {
            System.err.println("Error al conectar con la API externa: " + e.getMessage());
        }
    }
}
