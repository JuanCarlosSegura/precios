package com.gasolineras.precios.consumer;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
public class GasolinerasConsumer {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "gasolineras-stream", groupId = "sevilla-group")
    public void procesarClima(String mensaje) {
        try {
            JSONObject apiJson = new JSONObject(mensaje);
            JSONObject current = apiJson.getJSONObject("current");

            JSONObject datosFiltrados = new JSONObject();
            datosFiltrados.put("ciudad", "Sevilla");
            datosFiltrados.put("temperatura", current.getDouble("temperature_2m"));
            datosFiltrados.put("humedad", current.getInt("relative_humidity_2m"));
            datosFiltrados.put("viento", current.getDouble("wind_speed_10m"));
            datosFiltrados.put("hora", current.getString("time"));

            messagingTemplate.convertAndSend("/topic/sevilla", datosFiltrados.toString());
        } catch (Exception e) {
            System.err.println("Mensaje Kafka ignorado por formato inválido: " + e.getMessage());
        }
    }
}
