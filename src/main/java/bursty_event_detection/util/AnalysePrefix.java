package bursty_event_detection.util;

import java.util.ArrayList;

public class AnalysePrefix {

    // 我们如何根据一个prefix数组来判断当前的趋势?
    // demo: prefix[date]比前一天突然增加了很多
    // policy: 从date开始算起，往前看三天，如果这三天中每天这个单词的出现数量都超过了m
    // 并且这三天中出现的总数超过了n(n > 3m)，那么可以认定这个单词是当下的热词
    // date是从1开始的
    public static boolean isTrending(ArrayList<Integer> list, int date) { // date >= 3
        if (date < 3) return false;
        int day1 = list.get(date - 2);
        int day2 = list.get(date - 1);
        int day3 = list.get(date);
        int sum = day1 + day2 + day3;
        int GRANULARITY = 1;
        if ((day1 >= GRANULARITY && day2 >= GRANULARITY && day3 >= GRANULARITY && sum >= 3*GRANULARITY + 1) || day3 >= 3*GRANULARITY) return true;
        return false;
    }

}
