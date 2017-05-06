package org.lightfor.log.es;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 测试性能
 * Created by Light on 2017/4/12.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger("1");

    public static void main(String[] args) {
        for (int i = 0; i < 1; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    long count = 0;
                    while (true) {
                        count++;
                        if(count % 100000 == 0){
                            logger.info("{}", count);
                        }
                        logger.debug("{}", count);
                    }
                }
            };

            thread.start();
        }

        while (true){

        }

    }
}
