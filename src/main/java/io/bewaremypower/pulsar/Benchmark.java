/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.bewaremypower.pulsar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Benchmark {

    private final Map<String, List<Long>> results = new HashMap<>();

    public long run(String name, KeyValueReader reader, String topic, Map<String, Integer> expectedResult)
            throws Exception {
        final var start = System.currentTimeMillis();
        final var result = reader.read(topic);
        final var elapsed = System.currentTimeMillis() - start;
        if (!result.equals(expectedResult)) {
            System.out.println("Result: " + mapToString(result) + ", expected: " + mapToString(result));
        }
        results.computeIfAbsent(name, __ -> new ArrayList<>()).add(elapsed);
        return elapsed;
    }

    public void print() {
        System.out.println("---- SUMMARY ----");
        results.forEach((name, times) -> System.out.println("# " + name + "\n" + listToString(times)));
    }

    private static String listToString(List<Long> list) {
        return String.join(", ", list.stream().map(Object::toString).toList());
    }

    static String mapToString(Map<String, Integer> map) {
        return String.join(", ",
                map.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).toList());
    }
}
