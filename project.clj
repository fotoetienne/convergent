(defproject convergent "0.1.0-SNAPSHOT"
  :description "Convergent Datatypes"
  :url "https://github.com/fotoetienne/convergent"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :min-lein-version "2.5.2"
  :jvm-opts ["-Xmx1g"]

  :dependencies [[org.clojure/core.match "0.3.0-alpha4"]]

  :profiles
  {:dev {:dependencies [[org.clojure/clojure "1.9.0-alpha11"]
                        [org.clojure/clojurescript "1.9.227"]
                        [org.clojure/test.check "0.9.0"]
                        [org.mozilla/rhino "1.7.7.1"]]
         :plugins [[lein-cljsbuild "1.1.3"]
                   [lein-doo "0.1.6"]]}}

  :aliases {"test-clj" ["test"]
            "test-cljs" ["doo" "rhino" "test" "once"]
            "test-all" ["do" "clean," "test-clj," "test-cljs"]
            "deploy" ["do" "clean," "deploy" "clojars"]}

  :jar-exclusions [#"\.swp|\.swo|\.DS_Store"]

  :doo {:paths {:rhino "lein run -m org.mozilla.javascript.tools.shell.Main"}}

  :cljsbuild {:builds
              {:test {:source-paths ["src" "test"]
                      :compiler {:output-to "target/unit-test.js"
                                 :main 'convergent.runner
                                 :optimizations :whitespace}}}})
