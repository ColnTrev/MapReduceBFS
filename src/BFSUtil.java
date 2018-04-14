/**
 * Created by colntrev on 4/5/18.
 */
public class BFSUtil {
    public static String output(String connections, Integer distance, String status){
        StringBuilder sb = new StringBuilder();
        char delimiter = ';';
        sb.append(connections);
        sb.append(delimiter);
        sb.append(distance.toString());
        sb.append(delimiter);
        sb.append(status);
        return sb.toString();
    }
    public static String checkStatus(String s1, String s2){
        if(!s1.equals("WHITE") && s2.equals("WHITE")){
            return s1;
        }
        if(s1.equals("WHITE") && s2.equals("WHITE")){
            return s2;
        }
        if(s1.equals("GREY") && s2.equals("BLACK")){
            return s1;
        }
        if(s1.equals("BLACK") && s2.equals("GREY")){
            return s2;
        }
        return s1;
    }
}
