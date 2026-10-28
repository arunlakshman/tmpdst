In Martin Fowler's seminal book, ***Refactoring: Improving the Design of Existing Code*** (2nd Edition), the refactoring patterns are organized into functional categories.

### 1. First Set of Refactorings

These are the foundational refactorings that represent the core catalog.

* **Extract Function:** Turn a code fragment into its own function named after its intent.
* **Inline Function:** Replace a call to a function with the body of the function itself.
* **Extract Variable:** Turn a complex expression into a well-named local variable.
* **Inline Variable:** Replace references to a variable with its underlying expression.
* **Change Function Declaration:** Rename a function, add/remove parameters, or change its signature.
* **Encapsulate Variable:** Restrict direct access to data by wrapping it in getter and setter functions.
* **Rename Variable:** Rename a variable to better convey its purpose.
* **Introduce Parameter Object:** Group related data items into a single object passed as a parameter.
* **Combine Functions into Class:** Group functions that operate on common data into a class.
* **Combine Functions into Transform:** Group functions into a transformation function that takes source data and produces derived data.
* **Split Phase:** Divide code that does two different things into separate modules or steps.

---

### 2. Encapsulation

Refactorings aimed at hiding internal details and decoupling dependencies.

* **Encapsulate Record:** Wrap a raw record/dictionary structure in a class or data object.
* **Encapsulate Collection:** Return a read-only view or copy of a collection, and provide explicit add/remove methods.
* **Replace Primitive with Object:** Replace a simple primitive type (like a string or number) with a dedicated domain object.
* **Replace Temp with Query:** Move the logic calculating a temporary variable into a dedicated getter/query method.
* **Extract Class:** Move a subset of fields and methods from a bloated class into a new class.
* **Inline Class:** Move all features from an underused class into another class and delete the original.
* **Hide Delegate:** Create delegating methods on a class to hide its internal dependencies from clients.
* **Remove Middle Man:** Remove delegating methods if a class acts as a simple pass-through to another object.
* **Substitute Algorithm:** Replace the body of a function with a clearer, more efficient algorithm.

---

### 3. Moving Features

Refactorings focused on moving responsibilities between classes and contexts.

* **Move Function:** Move a function to the module or class where it accesses data most frequently.
* **Move Field:** Move a field to the class where it is used most often.
* **Move Statements into Function:** Move repeating code statements into an existing function they always accompany.
* **Move Statements to Callers:** Move statements out of a function to the calling sites when behavior diverges.
* **Replace Inline Code with Function Call:** Replace existing code with a call to an existing function that performs the same logic.
* **Slide Statements:** Rearrange code statements so related lines are placed next to each other.
* **Split Loop:** Divide a loop that performs two different tasks into two separate loops.
* **Replace Loop with Pipeline:** Replace a traditional loop with collection pipeline operations (like `map`, `filter`, `reduce`).
* **Remove Dead Code:** Delete code that is no longer used or reachable.

---

### 4. Organizing Data

Refactorings focused on handling values, references, and data relationships.

* **Split Variable:** Divide a variable assigned more than once (and not acting as a loop variable/accumulator) into separate variables for each responsibility.
* **Rename Field:** Change the name of a field across a record or class structure.
* **Replace Derived Variable with Query:** Remove a variable that stores calculated data and replace it with a calculation query.
* **Change Reference to Value:** Convert a mutable reference object into an immutable value object.
* **Change Value to Reference:** Convert an immutable value object into a shared reference object.

---

### 5. Simplifying Conditional Logic

Refactorings designed to clean up branching and boolean expressions.

* **Decompose Conditional:** Extract the condition and the branches into separate, well-named functions.
* **Consolidate Conditional Expression:** Combine multiple conditional checks that yield the same result into a single expression.
* **Replace Nested Conditional with Guard Clauses:** Use early returns (guard clauses) to handle special cases instead of deeply nested `if/else` checks.
* **Replace Conditional with Polymorphism:** Replace conditional logic that branches based on type/state with polymorphic class hierarchies.
* **Introduce Special Case:** Use a Null Object or specialized subclass to encapsulate handling of default/special cases.
* **Introduce Assertion:** Use assertions to explicitly document and enforce assumptions in code.

---

### 6. Refactoring APIs

Refactorings centered on clean interface design and argument passing.

* **Separate Query from Modifier:** Ensure functions either return a value (query) or change state (modifier), but not both.
* **Parameterize Function:** Combine multiple functions with similar logic into a single function that takes a parameter.
* **Remove Flag Argument:** Replace boolean or status flag parameters with explicit, separate functions.
* **Preserve Whole Object:** Pass an entire object into a function rather than extracting and passing its individual fields.
* **Replace Parameter with Query:** Remove a parameter if the receiving function can calculate or query the value itself.
* **Replace Query with Parameter:** Pass a value explicitly into a function to decouple it from external state or queries.
* **Remove Setting Method:** Remove setter methods for fields that should be immutable after construction.
* **Replace Constructor with Factory Function:** Replace direct instantiation calls with a factory method.
* **Replace Function with Command:** Encapsulate a complex function execution inside its own command object.
* **Replace Command with Function:** Convert a small, simple command object back into a straightforward function.

---

### 7. Dealing with Inheritance

Refactorings for restructuring class hierarchies and object-oriented designs.

* **Pull Up Method:** Move a method from subclasses into their shared superclass.
* **Pull Up Field:** Move a field from subclasses into their shared superclass.
* **Pull Up Constructor Body:** Move common constructor logic from subclasses into the superclass constructor.
* **Push Down Method:** Move a method from a superclass down to the specific subclass where it is used.
* **Push Down Field:** Move a field from a superclass down to the specific subclass where it is used.
* **Replace Type Code with Subclasses:** Replace a primitive type/status field with a class hierarchy.
* **Remove Subclass:** Replace an underused or redundant subclass with fields or flags in the superclass.
* **Extract Superclass:** Create a shared superclass for two or more classes that share common features.
* **Collapse Hierarchy:** Merge a superclass and subclass together if they are no longer distinct.
* **Replace Subclass with Delegate:** Replace inheritance with composition by delegating subclass-specific behavior to an object.
* **Replace Superclass with Delegate:** Replace inheritance with composition when inheriting from a class whose interface shouldn't be exposed entirely.
