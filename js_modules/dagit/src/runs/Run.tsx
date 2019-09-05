import * as React from "react";
import * as yaml from "yaml";
import gql from "graphql-tag";
import styled from "styled-components";
import { IconNames } from "@blueprintjs/icons";
import { Colors } from "@blueprintjs/core";
import { MutationFunction, Mutation } from "react-apollo";
import ApolloClient from "apollo-client";

import LogsFilterProvider, {
  ILogFilter,
  GetDefaultLogFilter
} from "./LogsFilterProvider";
import LogsScrollingTable from "./LogsScrollingTable";
import {
  RunFragment,
  RunFragment_logs_nodes_ExecutionStepFailureEvent,
  RunFragment_executionPlan
} from "./types/RunFragment";
import { PanelDivider } from "../PanelDivider";
import { ExecutionPlan } from "../plan/ExecutionPlan";
import RunMetadataProvider from "../RunMetadataProvider";
import LogsToolbar from "./LogsToolbar";
import {
  HANDLE_START_EXECUTION_FRAGMENT,
  handleStartExecutionResult
} from "./RunUtils";
import { Reexecute, ReexecuteVariables } from "./types/Reexecute";
import RunSubscriptionProvider from "./RunSubscriptionProvider";
import { RunStatusToPageAttributes } from "./RunStatusToPageAttributes";
import ExecutionStartButton from "../execute/ExecutionStartButton";
import InfoModal from "../InfoModal";
import PythonErrorInfo from "../PythonErrorInfo";

interface IRunProps {
  client: ApolloClient<any>;
  run?: RunFragment;
}

interface IRunState {
  logsVW: number;
  logsFilter: ILogFilter;
  highlightedError?: { message: string; stack: string[] };
}

export class Run extends React.Component<IRunProps, IRunState> {
  static fragments = {
    RunFragment: gql`
      fragment RunFragment on PipelineRun {
        ...RunStatusPipelineRunFragment
        ...RunSubscriptionPipelineRunFragment

        __typename
        environmentConfigYaml
        runId
        mode
        pipeline {
          name
          solids {
            name
          }
        }
        logs {
          nodes {
            __typename
            ...LogsFilterProviderMessageFragment
            ...LogsScrollingTableMessageFragment
            ...RunMetadataProviderMessageFragment
            ... on ExecutionStepFailureEvent {
              step {
                key
              }
              error {
                stack
                message
              }
            }
          }
        }
        executionPlan {
          ...ExecutionPlanFragment
          steps {
            key
            inputs {
              dependsOn {
                key
                outputs {
                  name
                  type {
                    name
                  }
                }
              }
            }
          }
        }
        stepKeysToExecute
      }

      ${ExecutionPlan.fragments.ExecutionPlanFragment}
      ${LogsFilterProvider.fragments.LogsFilterProviderMessageFragment}
      ${LogsScrollingTable.fragments.LogsScrollingTableMessageFragment}
      ${RunStatusToPageAttributes.fragments.RunStatusPipelineRunFragment}
      ${RunMetadataProvider.fragments.RunMetadataProviderMessageFragment}
      ${RunSubscriptionProvider.fragments.RunSubscriptionPipelineRunFragment}
    `,
    RunPipelineRunEventFragment: gql`
      fragment RunPipelineRunEventFragment on PipelineRunEvent {
        ...LogsScrollingTableMessageFragment
        ...LogsFilterProviderMessageFragment
        ...RunMetadataProviderMessageFragment
      }

      ${RunMetadataProvider.fragments.RunMetadataProviderMessageFragment}
      ${LogsFilterProvider.fragments.LogsFilterProviderMessageFragment}
      ${LogsScrollingTable.fragments.LogsScrollingTableMessageFragment}
    `
  };

  state: IRunState = {
    logsVW: 75,
    logsFilter: GetDefaultLogFilter(),
    highlightedError: undefined
  };

  onShowStateDetails = (stepKey: string) => {
    const { run } = this.props;
    if (!run) return;

    const errorNode = run.logs.nodes.find(
      node =>
        node.__typename === "ExecutionStepFailureEvent" &&
        node.step != null &&
        node.step.key === stepKey
    ) as RunFragment_logs_nodes_ExecutionStepFailureEvent;

    if (errorNode) {
      this.setState({ highlightedError: errorNode.error });
    }
  };

  onReexecute = async (
    mutation: MutationFunction<Reexecute, ReexecuteVariables>,
    stepKey?: string
  ) => {
    const { run } = this.props;
    if (!run) return;

    const variables: ReexecuteVariables = {
      executionParams: {
        mode: run.mode,
        environmentConfigData: yaml.parse(run.environmentConfigYaml),
        selector: {
          name: run.pipeline.name,
          solidSubset: run.pipeline.solids.map(s => s.name)
        }
      }
    };

    if (stepKey) {
      const step = run.executionPlan.steps.find(s => s.key === stepKey);
      if (!step) return;

      variables.executionParams.stepKeys = [stepKey];
      variables.reexecutionConfig = {
        previousRunId: run.runId,
        stepOutputHandles: []
      };

      step.inputs.forEach(input => {
        const deps = input.dependsOn;
        deps.forEach(dep => {
          dep.outputs.forEach(outputOfDependentStep => {
            variables.reexecutionConfig!.stepOutputHandles.push({
              stepKey: dep.key,
              outputName: outputOfDependentStep.name
            });
          });
        });
      });
    }

    const result = await mutation({ variables });

    handleStartExecutionResult(run.pipeline.name, result, {
      openInNewWindow: false
    });
  };

  render() {
    const { client, run } = this.props;
    const { logsFilter, logsVW, highlightedError } = this.state;

    const logs = run ? run.logs.nodes : undefined;
    const stepKeysToExecute: (string | null)[] | null = run
      ? run.stepKeysToExecute
      : null;

    const executionPlan: RunFragment_executionPlan = run
      ? run.executionPlan
      : { __typename: "ExecutionPlan", steps: [], artifactsPersisted: false };

    return (
      <Mutation<Reexecute, ReexecuteVariables> mutation={REEXECUTE_MUTATION}>
        {reexecuteMutation => (
          <RunWrapper>
            {run && <RunSubscriptionProvider client={client} run={run} />}
            {run && <RunStatusToPageAttributes run={run} />}
            <LogsContainer style={{ width: `${logsVW}vw`, minWidth: 680 }}>
              <LogsFilterProvider filter={logsFilter} nodes={logs}>
                {({ filteredNodes, busy }) => (
                  <>
                    <LogsToolbar
                      showSpinner={busy}
                      filter={logsFilter}
                      onSetFilter={filter =>
                        this.setState({ logsFilter: filter })
                      }
                    >
                      <ExecutionStartButton
                        title="Re-execute"
                        icon={IconNames.REPEAT}
                        small={true}
                        onClick={() => this.onReexecute(reexecuteMutation)}
                      />
                    </LogsToolbar>
                    <LogsScrollingTable nodes={filteredNodes} />
                  </>
                )}
              </LogsFilterProvider>
            </LogsContainer>
            <PanelDivider
              onMove={(vw: number) => this.setState({ logsVW: vw })}
              axis="horizontal"
            />
            <RunMetadataProvider logs={logs || []}>
              {metadata => (
                <ExecutionPlan
                  runMetadata={metadata}
                  executionPlan={executionPlan}
                  stepKeysToExecute={stepKeysToExecute}
                  onShowStateDetails={this.onShowStateDetails}
                  onReexecuteStep={stepKey =>
                    this.onReexecute(reexecuteMutation, stepKey)
                  }
                  onApplyStepFilter={stepKey =>
                    this.setState({
                      logsFilter: {
                        ...this.state.logsFilter,
                        text: `step:${stepKey}`
                      }
                    })
                  }
                />
              )}
            </RunMetadataProvider>
            {highlightedError && (
              <InfoModal
                onRequestClose={() =>
                  this.setState({ highlightedError: undefined })
                }
              >
                <PythonErrorInfo error={highlightedError} />
              </InfoModal>
            )}
          </RunWrapper>
        )}
      </Mutation>
    );
  }
}

const RunWrapper = styled.div`
  display: flex;
  flex-direction: row;
  flex: 1 1;
  min-height: 0;
`;

const LogsContainer = styled.div`
  display: flex;
  flex-direction: column;
  background: ${Colors.LIGHT_GRAY5};
`;

const REEXECUTE_MUTATION = gql`
  mutation Reexecute(
    $executionParams: ExecutionParams!
    $reexecutionConfig: ReexecutionConfig
  ) {
    startPipelineExecution(
      executionParams: $executionParams
      reexecutionConfig: $reexecutionConfig
    ) {
      ...HandleStartExecutionFragment
    }
  }

  ${HANDLE_START_EXECUTION_FRAGMENT}
`;

export const PIPELINE_RUN_LOGS_UPDATE_FRAGMENT = gql`
  fragment PipelineRunLogsUpdateFragment on PipelineRun {
    runId
    status
    ...RunFragment
    logs {
      nodes {
        ...RunPipelineRunEventFragment
      }
    }
  }

  ${Run.fragments.RunFragment}
  ${Run.fragments.RunPipelineRunEventFragment}
`;

export const PIPELINE_RUN_LOGS_SUBSCRIPTION = gql`
  subscription PipelineRunLogsSubscription($runId: ID!, $after: Cursor) {
    pipelineRunLogs(runId: $runId, after: $after) {
      __typename
      ... on PipelineRunLogsSubscriptionSuccess {
        messages {
          ... on MessageEvent {
            runId
          }
          ...RunPipelineRunEventFragment
        }
      }
      ... on PipelineRunLogsSubscriptionMissingRunIdFailure {
        missingRunId
      }
    }
  }

  ${Run.fragments.RunPipelineRunEventFragment}
`;
