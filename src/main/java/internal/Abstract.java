package internal;

class Abstract<S extends Abstract<S>> {
    public S method() {
        return self();
    }

    @SuppressWarnings("unchecked")
    private S self() {
        return (S) this;
    }
}
