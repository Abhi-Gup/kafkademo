package com.example.kafkademo.model;

import java.util.Date;

public record Order(String itemName,String buyerName, int amount, Long orderId, String category) {

}