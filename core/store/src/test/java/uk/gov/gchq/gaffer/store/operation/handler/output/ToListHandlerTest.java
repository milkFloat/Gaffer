/*
 * Copyright 2017-2020 Crown Copyright
 *
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

package uk.gov.gchq.gaffer.store.operation.handler.output;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.store.Context;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ToListHandlerTest {

    @Test
    public void shouldConvertIterableToList() throws OperationException {
        // Given
        final List<Integer> originalList = Arrays.asList(1, 2, 3);

        final Iterable originalResults = new WrappedCloseableIterable<>(originalList);
        final ToListHandler handler = new ToListHandler();
        final ToList<Integer> operation = mock(ToList.class);

        given(operation.getInput()).willReturn(originalResults);

        //When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertEquals(originalList, results);
    }

    @Test
    public void shouldHandleNullInput() throws OperationException {
        // Given
        final ToListHandler handler = new ToListHandler();
        final ToList<Integer> operation = mock(ToList.class);

        given(operation.getInput()).willReturn(null);

        //When
        final Iterable<Integer> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(results).isNull();
    }
}
