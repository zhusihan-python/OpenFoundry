import { describe, expect, it } from 'vitest';

import { pipelineDAGWithNodes, pipelineNodesFromDAG, type PipelineIR } from './pipelines';

describe('pipelineNodesFromDAG', () => {
  const source = { id: 'source', label: 'Source', transform_type: 'external', config: {}, output_dataset_id: null };
  const child = {
    id: 'child',
    label: 'Child',
    transform_type: 'select',
    config: {},
    output_dataset_id: null,
    depends_on: ['source'],
    input_dataset_ids: ['input-b', 'input-a'],
  };

  it('fills omitted arrays in a legacy DAG without changing existing edges or input order', () => {
    const dag = [source, child];
    expect(pipelineNodesFromDAG(dag)).toEqual([
      { ...source, depends_on: [], input_dataset_ids: [] },
      child,
    ]);
    expect(dag[0]).not.toHaveProperty('depends_on');
    expect(dag[0]).not.toHaveProperty('input_dataset_ids');
  });

  it('handles null arrays in a Pipeline IR without dropping graph or node metadata', () => {
    const dag: PipelineIR = {
      ir_version: 'pipeline_ir.v1',
      nodes: [{ ...source, depends_on: null, input_dataset_ids: null, metadata: { source: 'saved' } }, child],
      edges: [{ id: 'edge', source_node_id: 'source', target_node_id: 'child' }],
      validation: { status: 'valid', errors: [] },
      version_metadata: { authoring_version: 3 },
    };

    expect(pipelineDAGWithNodes(dag, pipelineNodesFromDAG(dag))).toEqual({
      ...dag,
      nodes: [{ ...dag.nodes[0], depends_on: [], input_dataset_ids: [] }, child],
    });
    expect(dag.nodes[0].depends_on).toBeNull();
  });
});
