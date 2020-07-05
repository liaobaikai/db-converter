package com.liaobaikai.argparse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArgumentParser {



    /**
     * 所有的参数
     */
    private List<Argument> arguments = new ArrayList<Argument>();


    // The name of the program (default: Thread.currentThread().getStackTrace()[1].getFileName())
    private String program;

    // A usage message (default: auto-generated from arguments)
    private String usage;

    // A description of what the program does
    private String description;

    // Text following the argument descriptions
    private String epilog;

    // The default value for all arguments
    private String argumentDefault;

    // Add a -h/-help option
    private String addHelp;

    // 默认宽度
    private static final int DEFAULT_FORMAT_WIDTH = 22;

    public ArgumentParser() {
        this.description = "";
    }

    public ArgumentParser(String description) {
        this.description = description;
    }

    public Argument addArgument(String... flags){
        return new Argument(flags);
    }


    public Namespace parseArgs(String[] args){

        // --name 'test' --name 'test2' --p1 1 2 3

        Namespace ns = new Namespace();

        String value;
        for(int i = 0, len = args.length; i < len; i++){

            value = args[i].trim();

            if(value.length() == 0) continue;

            Argument argument = arguments.get(i);

            // 判断是位置参数还是可选参数、
            if(argument.isPositionalArgument()){
                for(String key : argument.getFlags()){
                    ns.put(key, value);
                }
            } else {
                for(String key : argument.getFlags()){
                    ns.put(key, value);
                }
            }

        }

        return ns;
    }


    public static void main(String[] args) {

        // https://docs.python.org/zh-cn/3/howto/argparse.html

        ArgumentParser parser = new ArgumentParser("");
        parser.addArgument("-t", "--type", "--data-type")
                .type(int.class)
                .help("参数类型");

        Namespace ns = parser.parseArgs(args);
        System.out.println(ns.getString("type"));

    }
}
