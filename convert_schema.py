
import sqlalchemy as sa
from sqlalchemy import schema, types, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_schemadisplay import create_schema_graph
from pyswip import Prolog
import argparse
from frozendict import frozendict

NONEQUIV_CONTROL_GROUP_DESC = """Nonequivalent Control Group Design
    ---------------------------------
    This design exploits pre-test and a post-test, but
    it cannot be assumed that the group receiving
    the treatment and the control group
    were equivalent before treatment was applied,
    so any differences in the post-test may actually be 
    a result of this inequivalence. Validity
    can be strengthened by finding a subset of the
    data set for which treatment is quasi-random.
    """

COUNTERBALANCED_DESC = """Counterbalanced Designs
    ---------------------------------
    These designs assume that treatment has been
    assigned on a rotating basis, and each unit
    has experienced each treatment.
    """

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rules", dest="rule_path", default=None)
    parser.add_argument("--db-source", dest="db_path", default="postgresql://dgarant@localhost:5432/movielens")

    args = parser.parse_args()

    if args.rule_path:
        with open(args.rule_path, 'r') as rule_handle:
            rules = [r.strip() for r in rule_handle.readlines()]
    else:
        rules = build_schema_rules(args.db_path)
    rules.extend(register_qeds())

    prolog = Prolog()
    for rule in rules:
        print(rule)
        prolog.assertz(rule)

    report_on_qeds(prolog, "movie_gross")

def report_on_qeds(prolog, outcome):
    """ Prints out suitable QEDs for modeling a particular outcome """
    nonequiv_control = list(get_unique_results(prolog, "nonequivControlGroup({0}, T)".format(outcome)))
    if nonequiv_control:
        print(NONEQUIV_CONTROL_GROUP_DESC)
        print("Candidate treatments for outcome {0}:".format(outcome))
        for elt in nonequiv_control:
            print("\t{0}".format(elt["T"]))
    
    print("\n")
    counterbalanced = list(get_unique_results(prolog, "counterbalancedDesign({0}, T)".format(outcome)))
    if counterbalanced:
        print(COUNTERBALANCED_DESC)
        print("Candidate treatments for outcome {0}:".format(outcome))
        for elt in counterbalanced:
            print("\t{0}".format(elt["T"]))

def get_unique_results(prolog, query_string):
    """ Creates a generator of unique query results """
    seen = set()
    for elt in prolog.query(query_string, catcherrors=False):
        frozenelt = frozendict(elt)
        if frozenelt in seen:
            continue
        else:
            seen.add(frozenelt)
            yield elt

def build_schema_rules(db_path):
    """ Connects to a database, analyzes its schema, 
        and constructs facts about that schema 
    """

    # connect to the database and reflect metadata
    engine = create_engine(db_path)
    metadata = schema.MetaData()
    metadata.reflect(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # build up a knowledge base from the metadata
    kb = []
    for t in metadata.sorted_tables:
        for fact in convert_table(session, t):
            register_fact(kb, fact)
        
        for col in t.columns:
            for fact in convert_attribute(session, t, col):
                register_fact(kb, fact)
            if col.primary_key:
                register_fact(kb, convert_pk(t, col))
            for fk in col.foreign_keys:
                for fact in convert_fk(session, t, fk):
                    register_fact(kb, fact)

    return kb


def register_qeds():
    """ Builds a report of applicable QEDs based on the 
        knowledge base stored in the Prolog engine
    """

    kb = []
    register_rule(kb, "tablesDirectlyRelated(X, Y) :- related(Y, X, R)")
    register_rule(kb, "tablesDirectlyRelated(X, Y) :- related(X, Y, R)")
    register_rule(kb, "tablesRelatedByPath(X, Y, P) :- tablesDirectlyRelated(X, Y)")
    register_rule(kb, "tablesRelatedByPath(X, Y, P) :- tablesDirectlyRelated(Z, X), \+ member(Z, P), tablesRelatedByPath(Z, Y, [X|P])")
    register_rule(kb, "tablesRelatedByPath(X, Y) :- tablesRelatedByPath(X, Y, [])")
    register_rule(kb, "attributesRelatedByPath(X, Y) :- attribute(X, T1), attribute(Y, T1)")
    register_rule(kb, "attributesRelatedByPath(X, Y) :- attribute(X, T1), attribute(Y, T2), tablesRelatedByPath(T1, T2)")
    register_rule(kb, "isNumeric(X) :- dataType(X, INTEGER)")
    register_rule(kb, "isNumeric(X) :- dataType(X, BIGINT)")
    register_rule(kb, "isNumeric(X) :- dataType(X, NUMERIC)")
    register_rule(kb, "variesWithTime(T, O) :- attribute(O, OTable), attributesRelatedByPath(T, O), attribute(E, OTable), " + 
                                             "dataType(E, time), attribute(E2, TTable), attribute(T, TTable), dataType(E2, time)")
    register_rule(kb, "suitableAsTreatment(T, O) :- attribute(O, T1), recordCount(T1, OutRecords), " + 
                      "isNumeric(O), levels(T, TreatLevels), TreatLevels < 30, OutRecords / TreatLevels > 20, T \= O")
    register_rule(kb, "nonequivControlGroup(Out, Treat) :- suitableAsTreatment(Treat, Out), variesWithTime(Treat, Out)")
    register_rule(kb, "counterbalancedDesign(Out, Treat) :- suitableAsTreatment(Treat, Out), variesWithTime(Treat, Out), levels(T, TreatLevels), TreatLevels > 3")
    register_rule(kb, "qed(Out, Treat) :- nonequivControlGroup(Out, Treat)")

    return kb

def register_rule(kb, rule):
    kb.append(rule)

def register_fact(kb, fact):
    kb.append(fact)

def convert_table(session, table):
    num_records = session.execute(table.count()).first()[0]
    return ["table({0})".format(to_identifier(str(table))),
            "recordCount({0}, {1})".format(
                to_identifier(str(table)), num_records)]


def convert_type(typename):
    if typename in ("INTEGER", "BIGINT", "NUMERIC"):
        return "numeric"
    elif typename.startswith("VARCHAR"):
        return "string"
    elif typename.startswith("TIMESTAMP"):
        return "time"
    else:
        raise ValueError("Unknown data type: {0}".format(typename))

def convert_attribute(session, table, attr):
    attr_label = to_identifier(str(attr)) 
    facts = ["attribute({0}, {1})".format(attr_label, table),
              "dataType({0}, {1})".format(attr_label, 
                        convert_type(str(attr.type)))]
    # number of distinct values gives the number of 
    # levels if this were to be a treatment
    num_distinct = session.query(sa.func.count(sa.distinct(attr))).first()[0]
    facts.append("levels({0}, {1})".format(attr_label, num_distinct))
    return facts

def convert_pk(table, key):
    return "primaryKey({0}, {1})".format(
        to_identifier(str(key)), table.name)

def convert_fk(session, table, key):
    oneColumn = key.column
    manyColumn = key.parent
    rname = key.name
    rules = ["related({0}, {1}, {2})".format(oneColumn.table, manyColumn.table, rname),
            "cardinality(OneCard, ManyCard, {0})".format(rname)]
    rules.append("key({0}, {1})".format(to_identifier(str(oneColumn)), rname))
    rules.append("key({0}, {1})".format(to_identifier(str(manyColumn)), rname))

    # select avg count of many-side elements, 
    # grouping by primary key of 1-side
    num_distinct_ref = session.query(oneColumn.table).join(manyColumn.table).group_by(oneColumn).count()
    total_rows = session.execute(manyColumn.table.count()).first()[0]
    rules.append("averageManySize({0}, {1})".format(rname, total_rows / float(num_distinct_ref)))
    return rules

def to_identifier(name):
    return name.replace(".", "_")
    
def create_schema_image(metadata):
    graph = create_schema_graph(metadata=metadata, 
        show_datatypes=True, show_indexes=False, rankdir='LR')
    import tempfile, Image
    with tempfile.NamedTemporaryFile(suffix=".png") as fout:
        graph.write(fout.name, format="png")
        Image.open(fout.name).show()

if __name__ == "__main__":
    main()


"""
"""
