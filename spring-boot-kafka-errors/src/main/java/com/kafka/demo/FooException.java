package com.kafka.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
public class FooException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private String msg;
}
