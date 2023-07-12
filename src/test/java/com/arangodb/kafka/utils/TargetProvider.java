package com.arangodb.kafka.utils;

import com.arangodb.ArangoCollection;
import com.arangodb.kafka.target.Connector;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.Target;
import com.arangodb.kafka.target.TestTarget;
import org.junit.jupiter.api.extension.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TargetProvider implements TestTemplateInvocationContextProvider {
    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        Target[] targetsArray = context.getRequiredTestMethod().getAnnotation(KafkaTest.class).value();
        if (targetsArray.length == 0) {
            targetsArray = Target.values();
        }
        List<Target> targets = Arrays.asList(targetsArray);

        String testNamePrefix = context.getRequiredTestClass().getSimpleName() + "-" + context.getRequiredTestMethod().getName() + "-";
        return Arrays.stream(Target.values())
                .filter(targets::contains)
                .map(it -> new TestTemplateInvocationContext() {
                    private final TestTarget target = it.create(testNamePrefix + it.name());

                    @Override
                    public String getDisplayName(int invocationIndex) {
                        return it.name();
                    }

                    @Override
                    public List<Extension> getAdditionalExtensions() {
                        return Arrays.asList(
                                new ParamSupplier<>(ArangoCollection.class, target::getCollection),
                                new ParamSupplier<>(Connector.class, () -> target),
                                new ParamSupplier<>(Producer.class, () -> target),
                                (BeforeTestExecutionCallback) extensionContext -> target.init(),
                                (AfterTestExecutionCallback) extensionContext -> target.close()
                        );
                    }
                });
    }

    static final class ParamSupplier<T> implements ParameterResolver {
        private final Class<T> clazz;
        private final Supplier<T> supplier;

        ParamSupplier(Class<T> clazz, Supplier<T> supplier) {
            this.clazz = clazz;
            this.supplier = supplier;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return parameterContext.getParameter().getType() == clazz;
        }

        @Override
        public T resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return supplier.get();
        }
    }
}
