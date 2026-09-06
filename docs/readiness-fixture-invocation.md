# One readiness fixture invocation

Primary arc42 block: `warehouse`. Goal #185; repair stacked on draft #184.

CI run 464 executed all eleven new readiness-source proofs successfully inside
`controlled_receiver_rehearsal_postgres_contract_check`. It then ran the readiness
fixture again through an added Makefile command. Fixed fixture request IDs now
referred to newly generated time-dependent evidence, so the recorder correctly
rejected the second invocation. Retrying the unchanged job cannot repair that
invocation graph.

Restore the accepted Makefile byte-for-byte. The existing controlled-receiver
command imports and invokes readiness history, which in turn runs the new source
proofs. The wiring regression checks that import/call path and rejects a second
direct Makefile invocation. No source proof, idempotency assertion, SQL validator
or database guard is removed.

The full PostgreSQL job must report all eleven `worker_readiness_source_proofs`
true and finish successfully. The new proof's temporary records remain subject
to its existing rollback; older fixture records remain in the disposable CI
service until normal teardown. This is not a change to production replay rules
or permission to rerun the fixture against an application database.

No schema, configuration, pinned workflow, notification, scheduler or deployment
change. This repair and its predecessor remain pending final-diff acceptance.
