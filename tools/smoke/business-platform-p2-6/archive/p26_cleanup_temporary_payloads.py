"""Post-seal exact two temporary-tree cleanup. No rm command or service IO.

CLI execution requires a new Root release and immutable external seal. Pure
controls call verification/deletion primitives only on NONSECRET temp fixtures.
"""
import argparse,datetime,hashlib,json,os,re,stat,sys
from pathlib import Path
sys.dont_write_bytecode=True
SOURCE='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
WORKSPACE=Path('/Volumes/ExternalSD/projects/OpenFoundry')
P26=WORKSPACE/'tools/smoke/business-platform-p2-6'
TARGETS=(P26/'results/w9/final-scan1-payload-staging-attempt1/payload',Path('/private/tmp/openfoundry-p26-final-independent-copy-attempt1/package'))
SCHEMA='p26-post-seal-temporary-cleanup-release-v1'
PRIVATE_ORIGINALS=frozenset(['tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T023539Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T023539Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025316Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025316Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025325Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025325Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025327Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025327Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025348Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025348Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025612Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025612Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025844Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025844Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025936Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T025936Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030657Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030657Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030726Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030726Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030736Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030736Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030831Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030832Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030843Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030843Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030859Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T030859Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T031523Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T031523Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T031525Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T031525Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T041930Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T041930Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T042343Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T042343Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T042407Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T042407Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T043559Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T043559Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T044014Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T044014Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T044032Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T044032Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T045223Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T045223Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T050412Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T050412Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T050523Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T050523Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051641Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051641Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051709Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051709Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051759Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051759Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051806Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051806Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051845Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051845Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051846Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051846Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051853Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051853Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051904Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051904Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051906Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051906Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051913Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051913Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051924Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051924Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051925Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051925Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051935Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T051935Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052038Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052038Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052039Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052039Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052051Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052051Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052122Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052122Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052123Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052123Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052516Z/01-go-test-2.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-connector-normal_20261115T052516Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T023539Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T025316Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T025325Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T025327Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T025348Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T025612Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T025844Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T025936Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T030657Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T030726Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T030736Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T030831Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T030843Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T030859Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T031523Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T031525Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T041930Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T042343Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T042407Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T043559Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T044014Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T044032Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T045223Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T050412Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T050523Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051641Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051709Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051759Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051806Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051845Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051846Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051853Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051904Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051906Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051913Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051924Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051925Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T051935Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T052038Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T052039Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T052051Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T052122Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T052123Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-dataset-race_20261115T052516Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-connector-normal_20261115T052122Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-connector-normal_20261115T052123Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-connector-normal_20261115T052516Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T051904Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T051906Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T051913Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T051924Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T051926Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T051935Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T052038Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T052039Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T052051Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T052122Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T052123Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w6/pg/test-unified-pipeline-normal_20261115T052516Z/01-go-test.raw', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/a_then_b_recover_a_head_unchanged-20261115T111106Z/018-05-a-proof-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/a_then_b_recover_a_head_unchanged-20261115T111106Z/028-07-b-proof-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/budget_exhausted_then_lookup_finalize-20261115T110929Z/077-12-budget-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/dropack_auto_retry_exact_replay-20261115T105934Z/025-10-dropack-auto-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/dropack_manual_owned_restart_lookup-20261115T110427Z/075-20-dropack-mr-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/healthy_incremental_epoch_once-20261115T105857Z/028-12-inc-final-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/healthy_snapshot_and_legacy-20261115T105817Z/019-07-healthy-snap-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/inflight_receipt404_samebody_replay-20261115T110605Z/033-15-inflight-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/pregate_no_intent_owned_restart-20261115T110247Z/071-17-pregate-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-attempts/twoinstances_exactly_one_claim-20261115T110755Z/041-12-twoinst-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-cross-CURRENT-supplement-20261115T112605Z/009-CURRENT-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/S2-cross-CURRENT-supplement-20261115T112835Z/009-CURRENT-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/a_then_b_recover_a_head_unchanged-20261115T074334Z/018-05-a-proof-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/a_then_b_recover_a_head_unchanged-20261115T074334Z/028-07-b-proof-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/budget_exhausted_then_lookup_finalize-20261115T074158Z/079-12-budget-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/crossscope-current-output-proof-20261115T074928Z/010-current-crossscope-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/dropack_auto_retry_exact_replay-20261115T065903Z/025-10-dropack-auto-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/dropack_auto_retry_exact_replay-20261115T070904Z/025-10-dropack-auto-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/dropack_manual_owned_restart_lookup-20261115T072721Z/075-20-dropack-mr-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/healthy_incremental_epoch_once-20261115T065842Z/028-12-inc-final-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/healthy_incremental_epoch_once-20261115T070846Z/028-12-inc-final-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/healthy_snapshot_and_legacy-20261115T065815Z/019-07-healthy-snap-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/healthy_snapshot_and_legacy-20261115T070829Z/019-07-healthy-snap-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/inflight_receipt404_samebody_replay-20261115T073839Z/033-15-inflight-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/pregate_no_intent_owned_restart-20261115T072854Z/071-17-pregate-file-bytes.cmd', 'tools/smoke/business-platform-p2-6/results/w7/attempts/twoinstances_exactly_one_claim-20261115T074016Z/041-12-twoinst-file-bytes.cmd'])


def need(ok,reason):
    if not ok:raise ValueError(reason)


def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(data):return hashlib.sha256(data).hexdigest()


def relative(value):
    need(isinstance(value,str) and re.fullmatch(r'[A-Za-z0-9._+-]+(?:/[A-Za-z0-9._+-]+)*',value)
         and all(v not in ('','.','..') for v in value.split('/')),'unsafe relative member')
    need(not any(v.lower().startswith('.env') or v.lower().endswith(('.env','.body')) or '.env.' in v.lower() for v in value.split('/')),'private env/body name forbidden')
    return value


def canonical(value):
    need(isinstance(value,str) and Path(value).is_absolute(),'absolute canonical path required')
    p=Path(value);need(p==p.resolve(),'symlink/noncanonical path')
    need(not any(v.is_symlink() for v in [p,*p.parents]),'symlink ancestry')
    return p


def overlap(a,b):return a==b or a.is_relative_to(b) or b.is_relative_to(a)


def public_metadata_path(path):
    need(path.name.endswith('.json') and not any(v.lower().startswith('.env') or v.lower().endswith(('.env','.body')) or '.env.' in v.lower() for v in path.parts),'only public JSON metadata; private env/body forbidden before open')


def read_phase(path):
    public_metadata_path(path);fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        before=os.fstat(fd);need(stat.S_ISREG(before.st_mode),'phase nonregular')
        with os.fdopen(fd,'rb',closefd=False) as f:data=f.read()
        after=os.fstat(fd);need(identity(before)==identity(after),'phase readrace')
        return data
    finally:os.close(fd)


def read_ref(ref):
    need(isinstance(ref,dict) and set(ref)=={'path','sha256','size'} and type(ref['size']) is int and ref['size']>=0
         and isinstance(ref['sha256'],str) and re.fullmatch('[a-f0-9]{64}',ref['sha256']),'exact public metadata ref')
    p=canonical(ref['path']);public_metadata_path(p)
    fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        st=os.fstat(fd);need(stat.S_ISREG(st.st_mode),'metadata nonregular')
        with os.fdopen(fd,'rb',closefd=False) as stream:data=stream.read()
        after=os.fstat(fd);need((st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns),'metadata changed while reading')
    finally:os.close(fd)
    need(len(data)==ref['size'] and sha(data)==ref['sha256'],'metadata pin drift')
    return json.loads(data),data


def pin_ok(pin):
    return isinstance(pin,dict) and set(pin)=={'sha256','size'} and type(pin['size']) is int and pin['size']>=0 and isinstance(pin['sha256'],str) and re.fullmatch('[a-f0-9]{64}',pin['sha256'])


def archive_members(doc):
    need(doc['format']=='p26-archive-manifest-v1' and isinstance(doc['files'],list) and doc['files'],'official archive manifest required')
    entries={}
    for item in doc['files']:
        need(set(item)=={'path','sha256','size'},'manifest row shape');path=relative(item['path']);pin={k:item[k] for k in ('sha256','size')}
        need(path!='manifest.json' and path not in entries and pin_ok(pin),'manifest duplicate/self/invalid row');entries[path]=pin
    payload=''.join(f"{entries[path]['sha256']}  {entries[path]['size']}  {path}\n" for path in sorted(entries)).encode()
    need(type(doc['file_count']) is int and doc['file_count']==len(entries) and doc['manifest_sha256']==sha(payload),'official canonical manifest recipe/count')
    return entries


def directories(members):
    result={''}
    for path in members:
        parts=relative(path).split('/')
        result.update('/'.join(parts[:n]) for n in range(1,len(parts)))
    return result


def identity(st):return (st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns)


def open_root(root):
    root=canonical(str(root));fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    need(stat.S_ISDIR(os.fstat(fd).st_mode),'existing directory required')
    return fd


def parent_fd(rootfd,path,expected_dirs=None):
    parts=relative(path).split('/');fd=os.dup(rootfd);prefix=''
    try:
        if expected_dirs is not None:
            st=os.fstat(fd);need((st.st_dev,st.st_ino)==expected_dirs[''],'root directory identity drift')
        for part in parts[:-1]:
            child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd);os.close(fd);fd=child
            prefix=prefix+'/'+part if prefix else part
            if expected_dirs is not None:
                st=os.fstat(fd);need((st.st_dev,st.st_ino)==expected_dirs[prefix],'directory identity drift')
        return fd,parts[-1]
    except BaseException:os.close(fd);raise


def file_pin(rootfd,path,expected_dirs=None):
    pfd,name=parent_fd(rootfd,path,expected_dirs)
    try:
        fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=pfd)
        try:
            before=os.fstat(fd);need(stat.S_ISREG(before.st_mode),'member nonregular')
            h=hashlib.sha256()
            with os.fdopen(fd,'rb',closefd=False) as f:
                for chunk in iter(lambda:f.read(1048576),b''):h.update(chunk)
            after=os.fstat(fd);need(identity(before)==identity(after),'member readrace')
            return {'sha256':h.hexdigest(),'size':after.st_size},identity(after)
        finally:os.close(fd)
    finally:os.close(pfd)


def verify_tree(root,expected,deny):
    """Exact regular files AND exact derived directories, before any deletion."""
    need(isinstance(expected,dict) and expected and all(pin_ok(v) for v in expected.values()),'nonempty exact member pins')
    need(not set(expected)&set(deny),'private original member forbidden')
    for path in expected:relative(path)
    rootfd=open_root(root);rootst=os.fstat(rootfd);gotfiles=set();gotdirs={''};dirids={'':(rootst.st_dev,rootst.st_ino)};fileids={}
    def visit(fd,prefix):
        for name in os.listdir(fd):
            path=prefix+'/'+name if prefix else name;relative(path)
            st=os.stat(name,dir_fd=fd,follow_symlinks=False)
            need(not stat.S_ISLNK(st.st_mode),'symlink tree refused')
            if stat.S_ISDIR(st.st_mode):
                gotdirs.add(path);dirids[path]=(st.st_dev,st.st_ino)
                child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
                try:visit(child,path)
                finally:os.close(child)
            else:
                need(stat.S_ISREG(st.st_mode) and path in expected,'unlisted/nonregular member')
                pin,ident=file_pin(rootfd,path);need(pin==expected[path],'member byte drift');gotfiles.add(path);fileids[path]=ident
    try:visit(rootfd,'')
    finally:os.close(rootfd)
    need(gotfiles==set(expected) and gotdirs==directories(expected),'exact files/directories differ, including extra empty directory')
    return {'root':str(root),'expected':expected,'files':fileids,'dirs':dirids,'root_identity':dirids['']}


def delete_verified(proof,phase_unchanged,progress=None):
    """Only verified files and known empty directories. Parents are never removed."""
    root=Path(proof['root']);rootfd=open_root(root);deleted=0;removeddirs=0
    progress = progress if progress is not None else {}
    progress.update(files_deleted=0,directories_deleted=0,target_exists_after=True)
    def root_same():
        phase_unchanged();st=os.lstat(root);need((st.st_dev,st.st_ino)==proof['root_identity'] and stat.S_ISDIR(st.st_mode),'target root drift')
    try:
        root_same()
        for path in sorted(proof['expected']):
            root_same();pin,ident=file_pin(rootfd,path,proof['dirs']);need(pin==proof['expected'][path] and ident==proof['files'][path],'member drift before unlink')
            pfd,name=parent_fd(rootfd,path,proof['dirs'])
            try:
                current=os.stat(name,dir_fd=pfd,follow_symlinks=False);need(stat.S_ISREG(current.st_mode) and identity(current)==ident,'member replace before unlink');os.unlink(name,dir_fd=pfd)
            finally:os.close(pfd)
            deleted+=1;progress['files_deleted']=deleted
        for path in sorted((v for v in proof['dirs'] if v),key=lambda x:(x.count('/'),x),reverse=True):
            root_same();pfd,name=parent_fd(rootfd,path,proof['dirs'])
            try:
                st=os.stat(name,dir_fd=pfd,follow_symlinks=False);need(stat.S_ISDIR(st.st_mode) and (st.st_dev,st.st_ino)==proof['dirs'][path],'directory drift')
                os.rmdir(name,dir_fd=pfd);removeddirs+=1;progress['directories_deleted']=removeddirs
            finally:os.close(pfd)
        root_same();need(not os.listdir(rootfd),'target not empty after exact unlink')
    finally:os.close(rootfd)
    os.rmdir(root);progress.update(files_deleted=deleted,directories_deleted=removeddirs+1,target_exists_after=root.exists());return progress


def prepare(release,output_created=False):
    need(release['schema']==SCHEMA and release['phase']=='p26-post-seal-exact-temporary-payload-cleanup' and release['root_accepted'] is True and release['source_sha256']==SOURCE,'explicit Root postseal cleanup release')
    need(not release.get('pending'),'final cleanup inputs must be complete')
    need(datetime.datetime.now(datetime.timezone.utc)<datetime.datetime.fromisoformat(release['expires_utc'].replace('Z','+00:00')),'expired cleanup phase')
    need(release['tool_sha256']==sha(Path(__file__).read_bytes()),'cleanup tool pin')
    rows=release['targets'];need(isinstance(rows,list) and len(rows)==2 and {v['path'] for v in rows}==set(map(str,TARGETS)),'only hard-fixed two temporary roots')
    official=canonical(release['official_root']);output=canonical(release['output_dir']);need(official.is_dir() and (output.is_dir() if output_created else not output.exists()),'official existing and new exclusive audit output')
    roots=[*TARGETS,official,output]
    need(all(not overlap(x,y) for n,x in enumerate(roots) for y in roots[n+1:]),'official/temp/audit overlap forbidden')
    need(output.is_relative_to(P26/'results/w9'),'bounded external cleanup audit leaf')
    for root in TARGETS:canonical(str(root));need(root.is_dir(),'target absent; replay cannot claim rc0')
    sealpath=canonical(release['seal']['path']);need(all(not sealpath.is_relative_to(root) for root in roots),'seal must be external and preserved')
    officialref=release['official_manifest'];need(canonical(officialref['path'])==official/'manifest.json','official manifest exact path')
    officialdoc,officialraw=read_ref(officialref);officialmembers=archive_members(officialdoc);seal,_=read_ref(release['seal'])
    need(seal['schema']=='p26-final-root-seal-v1' and seal['sealed'] is True and seal['source_sha256']==SOURCE
         and seal['official_package']==str(official) and seal['manifest_sha256']==officialdoc['manifest_sha256']
         and seal['package_manifest_file_sha256']==officialref['sha256'],'actual external Root seal/official manifest binding')
    deny=set(release['private_original_members']);need(deny==PRIVATE_ORIGINALS and len(release['private_original_members'])==173,'exact known private original denyset')
    for path in deny:relative(path)
    officialall=dict(officialmembers,**{'manifest.json':{'sha256':sha(officialraw),'size':len(officialraw)}})
    verify_tree(official,officialall,deny)
    proofs=[]
    for row in sorted(rows,key=lambda v:tuple(map(str,TARGETS)).index(v['path'])):
        root=Path(row['path']);ref=row['manifest'];need(not canonical(ref['path']).is_relative_to(output),'manifest cannot live in fresh audit output')
        doc,data=read_ref(ref)
        if root==TARGETS[0]:
            need(doc['schema']=='p26-secret-scan-members-v1' and doc['source_sha256']==SOURCE,'stage exact members manifest')
            expected=doc['members'];need(isinstance(expected,dict) and expected and all(q in officialmembers and pin==officialmembers[q] for q,pin in expected.items()),'every stage destination/hash/size must also be in permanent official package')
            need(not canonical(ref['path']).is_relative_to(root),'stage member manifest must remain outside payload')
        else:
            need(canonical(ref['path'])==root/'manifest.json' and data==officialraw,'independent manifest must be exact official manifest bytes')
            expected=dict(archive_members(doc),**{'manifest.json':{'sha256':sha(data),'size':len(data)}})
        need(type(row['expected_file_count']) is int and row['expected_file_count']==len(expected),'Root exact target member count')
        proofs.append(verify_tree(root,expected,deny))
    return output,proofs,officialdoc


def execute(release_file):
    releasepath=canonical(release_file);releasebytes=read_phase(releasepath);release=json.loads(releasebytes)
    # Create only a new bounded audit leaf after checking the signed action/scope.
    need(release['schema']==SCHEMA and release['root_accepted'] is True and release['phase']=='p26-post-seal-exact-temporary-payload-cleanup'
         and release['source_sha256']==SOURCE and release['tool_sha256']==sha(Path(__file__).read_bytes()) and not release.get('pending'),'Root cleanup release/tool binding')
    need(datetime.datetime.now(datetime.timezone.utc)<datetime.datetime.fromisoformat(release['expires_utc'].replace('Z','+00:00')),'expired cleanup phase')
    need(len(release['targets'])==2 and {v['path'] for v in release['targets']}==set(map(str,TARGETS)),'only exact two target roots')
    official=canonical(release['official_root']);output=canonical(release['output_dir'])
    need(all(not releasepath.is_relative_to(p) for p in [*TARGETS,official]),'authorizing phase must stay outside cleanup and permanent package trees')
    need(output.is_relative_to(P26/'results/w9') and not output.exists()
         and all(not overlap(output,p) for p in [*TARGETS,official])
         and all(not overlap(official,p) for p in TARGETS),'new audit outside official and both temporary trees')
    output.mkdir(parents=True,exist_ok=False);start=utc();counts=[];result=1;failure=None;officialdoc=None
    (output/'command.json').write_text(json.dumps({'argv':[sys.executable,'-I','-B',str(Path(__file__).absolute()),'--release-file',str(releasepath)],'started_utc':start,'release_sha256':sha(releasebytes)},indent=2)+'\n')
    (output/'Root-release.snapshot.json').write_bytes(releasebytes)
    def unchanged():
        need(read_phase(releasepath)==releasebytes,'Root cleanup phase changed')
        need(datetime.datetime.now(datetime.timezone.utc)<datetime.datetime.fromisoformat(release['expires_utc'].replace('Z','+00:00')),'cleanup phase expired before deletion')
    try:
        _,proofs,officialdoc=prepare(release,output_created=True)
        # Both complete trees and official bytes have been checked before deletion.
        for proof in proofs:verify_tree(Path(proof['root']),proof['expected'],PRIVATE_ORIGINALS)
        for proof in proofs:
            row={'target':proof['root'],'files_deleted':0,'directories_deleted':0};counts.append(row)
            delete_verified(proof,unchanged,row)
        result=0
    except BaseException as exc:failure=type(exc).__name__
    for row in counts:row['target_exists_after']=Path(row['target']).exists()
    report={'schema':'p26-exact-temporary-cleanup-result-v1','started_utc':start,'ended_utc':utc(),'original_rc':result,'failure_kind':failure,'targets':counts,
            'after_existence':{str(p):p.exists() for p in TARGETS},'official_manifest_sha256':officialdoc['manifest_sha256'] if officialdoc else None,'official_package_modified':False,
            'parents_audits_captures_profiles_removed':False,'private_originals_read_or_deleted':False,'services_called':0}
    (output/'RESULT.json').write_text(json.dumps(report,indent=2)+'\n');(output/'rc').write_text(str(result)+'\n');print(json.dumps(report));return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--release-file',required=True);args=parser.parse_args()
    try:return execute(args.release_file)
    except BaseException as exc:
        print(json.dumps({'status':'REFUSED-BEFORE-DELETE','failure_kind':type(exc).__name__,'original_rc':1}));return 1
if __name__=='__main__':sys.exit(main())
