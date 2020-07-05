package com.liaobaikai.argparse;

public class Argument {

    private String help;
    private String value;
    private String action;
    private String constValue;
    private String defaultValue;
    private Class<?> type;
    private String choices;
    private String required;
    private String[] flags;

    public Argument(String[] flags) {
        this.flags = flags;
    }

    public Argument action(String action){
        this.action = action;
        return this;
    }

    public Argument help(String help) {
        this.help = help;
        return this;
    }

    public Argument type(Class<?> clazz) {
        this.type = type;
        return this;
    }

    /**
     * 如果位置参数和可选参数并存的话，直接抛出 ParseException
     * @return
     */
    public boolean isPositionalArgument(){

        int validValue = 0;

        for (String s : flags) {
            if (s.startsWith("-")) {
                if (validValue == 0 || validValue == 2) {
                    validValue = 2;
                } else {
                    validValue = -1;

                }
            } else {
                if (validValue == 0 || validValue == 3) {
                    validValue = 3;
                } else {
                    validValue = -1;
                }
            }
        }

        if(validValue == -1){
            throw new ParseException("位置参数和可选参数不能并存");
        } else if(validValue == 0){
            throw new ParseException("参数flags为空?");
        }

        return validValue == 3;
    }

    public String getHelp() {
        return help;
    }

    public void setHelp(String help) {
        this.help = help;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getConstValue() {
        return constValue;
    }

    public void setConstValue(String constValue) {
        this.constValue = constValue;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    public String getChoices() {
        return choices;
    }

    public void setChoices(String choices) {
        this.choices = choices;
    }

    public String getRequired() {
        return required;
    }

    public void setRequired(String required) {
        this.required = required;
    }

    public String[] getFlags() {
        return flags;
    }

    public void setFlags(String[] flags) {
        this.flags = flags;
    }
}
