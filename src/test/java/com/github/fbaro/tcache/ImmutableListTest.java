package com.github.fbaro.tcache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

public class ImmutableListTest {

    @Test
    public void fullTest() {
        com.google.common.collect.ImmutableList<Integer> reference = ImmutableList.of(0, 1, 2, 3, 4, 5);
        com.github.fbaro.tcache.ImmutableList<Integer> mine = com.github.fbaro.tcache.ImmutableList.copyOf(reference);
        assertEquals(reference, mine);

        for (int from = 0; from <= reference.size(); from++) {
            for (int to = from; to <= reference.size(); to++) {
                assertEquals(reference.subList(from, to), mine.subList(from, to));
                assertEquals(Lists.reverse(reference.subList(from, to)), mine.subList(from, to).invert());
                assertEquals(Lists.reverse(reference).subList(from, to), mine.invert().subList(from, to));

                ImmutableList<Integer> refS = reference.subList(from, to);
                com.github.fbaro.tcache.ImmutableList<Integer> mineS = mine.subList(from, to);
                for (int from2 = 0; from2 <= refS.size(); from2++) {
                    for (int to2 = from2; to2 <= refS.size(); to2++) {
                        assertEquals(refS.subList(from2, to2), mineS.subList(from2, to2));
                        assertEquals(Lists.reverse(refS.subList(from2, to2)), mineS.subList(from2, to2).invert());
                        assertEquals(Lists.reverse(refS).subList(from2, to2), mineS.invert().subList(from2, to2));
                    }
                }
            }
        }
    }
}
