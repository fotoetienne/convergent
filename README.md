# Convergent

Convergent Replicated Data Types

## Usage

    (def a (gset #{1 2 3}))
    (def b (gset #{3 4 5}))
    (def c (join a b))
    (view c) ;; => #{1 2 3 4 5}

## Development

### Testing

To run tests for both Clojure and ClojureScript:

    lein test-all

## Requirements

Requires Clojure[Script] v1.9


## License

Copyright © 2017 Stephen Spalding

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
