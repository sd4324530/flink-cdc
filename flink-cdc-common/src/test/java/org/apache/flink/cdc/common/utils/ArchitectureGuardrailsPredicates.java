/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.common.utils;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared ArchUnit guardrail machinery of the per-Flink-version module architecture Lives in
 * flink-cdc-common's test-jar — the root of the dependency tree — so every main module can host a
 * thin guardrail test without new cross-module dependencies.
 *
 * <p>Two rules apply to the classes of the hosting module (identified by the module's directory
 * name, matched against the source URI prefix {@code <module>/target/classes}): main modules must
 * not define classes outside {@code org.apache.flink.cdc..} (rule 1) and must not directly
 * extend/implement the confined Flink APIs of {@code adapter-confined-apis.txt} (rule 2) —
 * subclassing must go through the version-module adapters. Classes registered in {@code
 * adapter-confined-exemptions.txt} are exempt from rule 2 as tracked legacy debt.
 */
public final class ArchitectureGuardrailsPredicates {

    static final String CONFINED_APIS_RESOURCE = "adapter-confined-apis.txt";
    static final String LEGACY_EXEMPTIONS_RESOURCE = "adapter-confined-exemptions.txt";

    private static volatile Set<String> confinedApiNames;
    private static volatile Set<String> exemptedClassNames;

    private ArchitectureGuardrailsPredicates() {}

    /**
     * Imports only class files under any {@code target/classes} directory on the classpath — i.e.
     * the main classes of the modules built in the current reactor session. Jar-sourced classes
     * (including the version modules' adapters and shadows, which legitimately live in Flink-API
     * packages) are never imported, and neither are test classes. The guardrail rules only inspect
     * main-module classes; supertype names of unimported types are still resolved from the bytecode
     * metadata.
     */
    static final class ModuleOutputImportOption implements ImportOption {

        @Override
        public boolean includes(Location location) {
            return location.asURI().toString().contains("/target/classes/");
        }
    }

    /**
     * Convenience entry point for the per-module thin-shell host tests: imports the classes of the
     * module-output directories found on the test classpath (see {@link ModuleOutputImportOption}),
     * evaluates both guardrail rules and the sanity check (the rules must see at least one class of
     * the hosting module).
     */
    public static void checkModuleGuardrails(String... moduleDirectoryNames) {
        JavaClasses classes =
                new ClassFileImporter()
                        .withImportOption(new ModuleOutputImportOption())
                        .importPackages("org.apache.flink");
        check(classes, moduleDirectoryNames);
    }

    /**
     * Convenience entry point: evaluates both guardrail rules and the sanity check (the rules must
     * see at least one class of the hosting module).
     */
    public static void check(JavaClasses classes, String... moduleDirectoryNames) {
        mainModulesStayInCdcPackages(moduleDirectoryNames).check(classes);
        mainModulesUseAdaptersForConfinedApis(moduleDirectoryNames).check(classes);
        long mainClasses =
                classes.stream().filter(mainModuleClasses(moduleDirectoryNames)::test).count();
        assertThat(mainClasses)
                .as(
                        "guardrail rules must see at least one main-module class of %s",
                        Arrays.toString(moduleDirectoryNames))
                .isPositive();
    }

    /** Rule 1: main modules must not define classes outside org.apache.flink.cdc... */
    public static ArchRule mainModulesStayInCdcPackages(String... moduleDirectoryNames) {
        return noClasses()
                .that(mainModuleClasses(moduleDirectoryNames))
                .should()
                .resideOutsideOfPackage("org.apache.flink.cdc..");
    }

    /**
     * Rule 2: main modules must not directly extend or implement the confined Flink APIs.
     * Transitive references through an adapter's own supertype chain are legal and therefore not
     * checked; only a direct supertype relationship bypasses the adapter layer.
     */
    public static ArchRule mainModulesUseAdaptersForConfinedApis(String... moduleDirectoryNames) {
        return classes()
                .that(mainModuleClasses(moduleDirectoryNames))
                .should(beFreeOfConfinedDirectSupertypes());
    }

    /** Matches classes compiled from the given modules' main sources, not from jars. */
    public static DescribedPredicate<JavaClass> mainModuleClasses(String... moduleDirectoryNames) {
        List<String> markers = new ArrayList<>();
        for (String moduleDirectory : moduleDirectoryNames) {
            markers.add(moduleDirectory + "/target/classes");
        }
        return new DescribedPredicate<>("originate from a main module's target/classes") {
            @Override
            public boolean test(JavaClass input) {
                return input.getSource()
                        .map(source -> source.getUri().toString())
                        .map(uri -> markers.stream().anyMatch(uri::contains))
                        .orElse(false);
            }
        };
    }

    /** Condition: the class is free of confined Flink APIs among its DIRECT supertypes. */
    public static ArchCondition<JavaClass> beFreeOfConfinedDirectSupertypes() {
        return new ArchCondition<>("be free of confined Flink APIs among its direct supertypes") {
            @Override
            public void check(JavaClass clazz, ConditionEvents events) {
                if (loadExemptedClassNames().contains(clazz.getName())) {
                    return;
                }
                Set<String> direct = new HashSet<>();
                clazz.getSuperclass().ifPresent(superclass -> direct.add(superclass.getName()));
                clazz.getInterfaces().forEach(i -> direct.add(i.getName()));
                for (String name : eraseGenerics(direct)) {
                    if (loadConfinedApiNames().contains(name)) {
                        events.add(
                                SimpleConditionEvent.violated(
                                        clazz,
                                        String.format(
                                                "%s directly extends/implements confined %s —"
                                                        + " subclassing must go through a"
                                                        + " version-module adapter; legacy"
                                                        + " exemptions are registered in %s",
                                                clazz.getName(),
                                                name,
                                                LEGACY_EXEMPTIONS_RESOURCE)));
                    }
                }
            }
        };
    }

    /** Supertype names may carry generic parameters; compare erasures. */
    static Set<String> eraseGenerics(Set<String> names) {
        Set<String> erased = new HashSet<>();
        for (String name : names) {
            int idx = name.indexOf('<');
            erased.add(idx < 0 ? name : name.substring(0, idx));
        }
        return erased;
    }

    static Set<String> loadConfinedApiNames() {
        Set<String> names = confinedApiNames;
        if (names == null) {
            names = new HashSet<>(readResourceLines(CONFINED_APIS_RESOURCE));
            confinedApiNames = names;
        }
        return names;
    }

    static Set<String> loadExemptedClassNames() {
        Set<String> names = exemptedClassNames;
        if (names == null) {
            Set<String> exemptions = new HashSet<>();
            for (String line : readResourceLines(LEGACY_EXEMPTIONS_RESOURCE)) {
                exemptions.add(line.split("\\|")[0].trim());
            }
            names = exemptions;
            exemptedClassNames = names;
        }
        return names;
    }

    static List<String> readResourceLines(String resourceName) {
        InputStream stream =
                ArchitectureGuardrailsPredicates.class
                        .getClassLoader()
                        .getResourceAsStream(resourceName);
        if (stream == null) {
            throw new IllegalStateException(
                    "Missing classpath resource " + resourceName + " (flink-cdc-common test-jar)");
        }
        try {
            List<String> lines = new ArrayList<>();
            for (String line :
                    new String(stream.readAllBytes(), StandardCharsets.UTF_8).split("\\R")) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                    lines.add(trimmed);
                }
            }
            return lines;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + resourceName, e);
        } finally {
            try {
                stream.close();
            } catch (IOException ignored) {
            }
        }
    }
}
