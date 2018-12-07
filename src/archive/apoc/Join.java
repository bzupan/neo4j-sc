    @Procedure
    @Description("apoc.cypher.run(fragment, params) yield value - executes reading fragment with the given parameters")
    public Stream<MapResult> run(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return db.execute(withParamMapping(statement, params.keySet()), params).stream().map(MapResult::new);
    }