#include "erl_nif.h"

#include <stdio.h>


static ERL_NIF_TERM atom_true;
static ERL_NIF_TERM atom_false;
static ERL_NIF_TERM badarg;



static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
    atom_true = enif_make_atom(env, "true");
    atom_false = enif_make_atom(env, "false");
    badarg = enif_make_badarg(env);
    return 0;
}

ERL_NIF_TERM is_sorted_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ERL_NIF_TERM head1, head2, tail, rest, list = argv[0];
    const ERL_NIF_TERM *tuple1, *tuple2;
    int arity1, arity2;

    if (!enif_is_list(env, list))
        return badarg;


    while(enif_get_list_cell(env, list, &head1, &tail) && enif_get_list_cell(env, tail, &head2, &rest)) {
        if(!enif_get_tuple(env, head1, &arity1, &tuple1) || !enif_get_tuple(env, head2, &arity2, &tuple2))
            return badarg;

        if (arity1 == 0 || arity2 == 0)
            return badarg;

        if (enif_compare(tuple1[0], tuple2[0]) >= 0)
            return atom_false;

        list = tail;
    }

    return atom_true;
}

static inline ErlNifSInt64 max(ErlNifSInt64 i1, ErlNifSInt64 i2) {
    if (i1 > i2)
        return i1;
    else
        return i2;
}

ERL_NIF_TERM merge2_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ERL_NIF_TERM headlhs, taillhs, listlhs = argv[0];
    ERL_NIF_TERM headrhs, tailrhs, listrhs = argv[1];
    ERL_NIF_TERM rretlist, retlist = enif_make_list(env, 0);
    ERL_NIF_TERM newtuple;

    const ERL_NIF_TERM *tuplelhs, *tuplerhs;
    int aritylhs, arityrhs, cmp;
    ErlNifSInt64 vallhs, valrhs;

    /* We will never receive an empty list either on the left or right side */

    while (enif_get_list_cell(env, listlhs, &headlhs, &taillhs) &&
           enif_get_list_cell(env, listrhs, &headrhs, &tailrhs)) {

        if(!enif_get_tuple(env, headlhs, &aritylhs, &tuplelhs))
            return badarg;
        if (aritylhs != 2)
            return badarg;

        if(!enif_get_tuple(env, headrhs, &arityrhs, &tuplerhs))
            return badarg;
        if (arityrhs != 2)
            return badarg;

        cmp = enif_compare(tuplelhs[0], tuplerhs[0]);
        if (cmp == 0) {
            if (!enif_get_int64(env, tuplelhs[1], &vallhs))
                return badarg;
            if (!enif_get_int64(env, tuplerhs[1], &valrhs))
                return badarg;

            newtuple = enif_make_tuple2(env, tuplelhs[0], enif_make_int64(env, max(vallhs, valrhs)));
            retlist = enif_make_list_cell(env, newtuple, retlist);
            /* Pop both lists */
            listlhs = taillhs;
            listrhs = tailrhs;
        } else if (cmp < 0) { /* lhs < rhs */
            /* Pop something off LHS to have it try to catch up with RHS */
            retlist = enif_make_list_cell(env, headlhs, retlist);
            listlhs = taillhs;
        } else if (cmp > 0) { /* lhs > rhs */
            /* Pop something off RHS to have it try to catch up with LHS */
            retlist = enif_make_list_cell(env, headrhs, retlist);
            listrhs = tailrhs;
        }
    }
    while (enif_get_list_cell(env, listlhs, &headlhs, &taillhs)) {
        retlist = enif_make_list_cell(env, headlhs, retlist);
        listlhs = taillhs;
    }

    while (enif_get_list_cell(env, listrhs, &headrhs, &tailrhs)) {
        retlist = enif_make_list_cell(env, headrhs, retlist);
        listrhs = tailrhs;
    }

    /* TODO: Maybe have some heuristic here to determine whether to do a reverse here, or in Erlang? */
    enif_make_reverse_list(env, retlist, &rretlist);
    return rretlist;
}

/* Return true if Lhs is a direct descendant of Rhs, else false -- remember, a vclock is its own descendant! */

ERL_NIF_TERM descends2_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ERL_NIF_TERM headlhs, taillhs, listlhs = argv[0];
    ERL_NIF_TERM headrhs, tailrhs, listrhs = argv[1];

    const ERL_NIF_TERM *tuplelhs, *tuplerhs;
    int aritylhs, arityrhs, cmp;
    ErlNifSInt64 vallhs, valrhs;

    while (enif_get_list_cell(env, listlhs, &headlhs, &taillhs) &&
           enif_get_list_cell(env, listrhs, &headrhs, &tailrhs)) {

        if(!enif_get_tuple(env, headlhs, &aritylhs, &tuplelhs))
            return badarg;
        if (aritylhs != 2)
            return badarg;

        if(!enif_get_tuple(env, headrhs, &arityrhs, &tuplerhs))
            return badarg;
        if (arityrhs != 2)
            return badarg;
        cmp = enif_compare(tuplelhs[0], tuplerhs[0]);
        if (cmp == 0) {
            if (!enif_get_int64(env, tuplelhs[1], &vallhs))
                return badarg;
            if (!enif_get_int64(env, tuplerhs[1], &valrhs))
                return badarg;
            if (vallhs < valrhs)
                return atom_false;
            listlhs = taillhs;
            listrhs = tailrhs;
        } else if (cmp < 0) {
            /* We have an extra actor on the lhs, drop it */
            listlhs = taillhs;
        } else
            return atom_false;
    }
    /* Make sure that RHS is empty */
    if(enif_get_list_cell(env, listrhs, &headrhs, &tailrhs))
        return atom_false;

    return atom_true;
}

ERL_NIF_TERM drop_dots_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ERL_NIF_TERM headlhs, taillhs, listlhs = argv[0];
    ERL_NIF_TERM headrhs, tailrhs, listrhs = argv[1];
    ERL_NIF_TERM rretlist, retlist = enif_make_list(env, 0);
    ERL_NIF_TERM newtuple;

    const ERL_NIF_TERM *tuplelhs, *tuplerhs;
    int aritylhs, arityrhs, cmp;
    ErlNifSInt64 vallhs, valrhs;

    /* We will never receive an empty list either on the left or right side */

    while (enif_get_list_cell(env, listlhs, &headlhs, &taillhs) &&
           enif_get_list_cell(env, listrhs, &headrhs, &tailrhs)) {

        if(!enif_get_tuple(env, headlhs, &aritylhs, &tuplelhs))
            return badarg;
        if (aritylhs != 2)
            return badarg;

        if(!enif_get_tuple(env, headrhs, &arityrhs, &tuplerhs))
            return badarg;
        if (arityrhs != 2)
            return badarg;

        cmp = enif_compare(tuplelhs[0], tuplerhs[0]);
        if (cmp == 0) {
            if (!enif_get_int64(env, tuplelhs[1], &vallhs))
                return badarg;
            if (!enif_get_int64(env, tuplerhs[1], &valrhs))
                return badarg;
            if (vallhs > valrhs) {
                newtuple = enif_make_tuple2(env, tuplelhs[0], enif_make_int64(env, vallhs));
                retlist = enif_make_list_cell(env, newtuple, retlist);
            }
            /* Pop both lists */
            listlhs = taillhs;
            listrhs = tailrhs;
        } else if (cmp < 0) { /* lhs < rhs */
            /* Pop something off LHS to have it try to catch up with RHS */
            retlist = enif_make_list_cell(env, headlhs, retlist);
            listlhs = taillhs;
        } else if (cmp > 0) { /* lhs > rhs */
            /* Pop something off RHS to have it try to catch up with LHS */
            listrhs = tailrhs;
        }
    }
    while (enif_get_list_cell(env, listlhs, &headlhs, &taillhs)) {
        retlist = enif_make_list_cell(env, headlhs, retlist);
        listlhs = taillhs;
    }

    /* TODO: Maybe have some heuristic here to determine whether to do a reverse here, or in Erlang? */
    enif_make_reverse_list(env, retlist, &rretlist);
    return rretlist;
}


static ErlNifFunc nif_funcs[] = {
    {"descends2", 2, descends2_nif},
    {"is_sorted", 1, is_sorted_nif},
    {"merge2", 2, merge2_nif},
    {"drop_dots", 2, drop_dots_nif},
};

ERL_NIF_INIT(riak_dt_vclock, nif_funcs, load, NULL, NULL, NULL)
