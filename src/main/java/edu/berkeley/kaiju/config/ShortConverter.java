package edu.berkeley.kaiju.config;

import com.beust.jcommander.IStringConverter;

public class ShortConverter implements IStringConverter<Short> {
    @Override
    public Short convert(String value) {
        return Short.parseShort(value);
    }
}