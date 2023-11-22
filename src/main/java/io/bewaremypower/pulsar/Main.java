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

public class Main {

    public static void main(String[] args) throws Exception {
        final int numMessages = args.length > 1 ? Integer.parseInt(args[0]) : 10;
        final var topic = args.length > 2 ? args[1] : "my-topic";
        System.out.println("Topic: " + topic + ", numMessages: " + numMessages);
        final var produceResult = PrepareData.run(topic, numMessages);
        System.out.println("Initial data: " + Benchmark.mapToString(produceResult));

        final var benchmark = new Benchmark();
        for (int i = 0; i < 3; i++) {
            long elapsed = benchmark.run("readNextAsync", () -> ReadNextAsync.read(topic), produceResult);
            System.out.println(i + " readNextAsync: " + elapsed + "ms");
            elapsed = benchmark.run("readNext", () -> ReadNext.read(topic), produceResult);
            System.out.println(i + " readNext: " + elapsed + "ms");
        }
        benchmark.print();
    }

}
