package util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.Vector;

/**
 * @author Mengsi He
 */
public class IKSegmenterTool {
    public static Vector<String> participle(String str ) {

        Vector<String> str1 = new Vector<String>() ;//对输入进行分词

        try {

            StringReader reader = new StringReader( str );
            IKSegmenter ik = new IKSegmenter(reader,true);//当为true时，分词器进行最大词长切分
            Lexeme lexeme = null ;

            while( ( lexeme = ik.next() ) != null ) {
                str1.add( lexeme.getLexemeText() );
            }

//            if( str1.size() == 0 ) {
//                return null ;
//            }

            //分词后
         //   System.out.println( "str分词后：" + str1 );

        } catch ( IOException e1 ) {
        //    System.out.println();
        }
        return str1;
    }
}
