public class Test1 {

    public static void main(String[] args) {

        System.out.println(String.format("jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&useSSL=true&serverTimezone=GMT%%2B8", "1, ", "1"));


    }
}
