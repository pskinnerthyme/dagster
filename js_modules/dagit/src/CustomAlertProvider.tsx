import * as React from "react";
import { Button, Dialog, Classes, Colors } from "@blueprintjs/core";
import styled from "styled-components";
import { copyValue } from "./Util";
import { HighlightedCodeBlock } from "./HighlightedCodeBlock";

const SHOW_ALERT_EVENT = "show-alert";

interface ICustomAlert {
  message: string;
  messageLang: string[];
  title: string;
  pre: boolean;
}

export const showCustomAlert = (opts: Partial<ICustomAlert>) => {
  document.dispatchEvent(
    new CustomEvent(SHOW_ALERT_EVENT, {
      detail: JSON.stringify(
        Object.assign({ message: "", title: "Error", pre: false }, opts)
      )
    })
  );
};

export default class CustomAlertProvider extends React.Component<
  {},
  Partial<ICustomAlert>
> {
  state: Partial<ICustomAlert> = {};

  componentDidMount() {
    document.addEventListener(SHOW_ALERT_EVENT, (e: CustomEvent) => {
      this.setState(JSON.parse(e.detail));
    });
  }

  render() {
    const { title, message, messageLang, pre } = this.state;
    return (
      <Dialog
        icon={title ? "info-sign" : undefined}
        usePortal={true}
        onClose={() => this.setState({ message: undefined })}
        style={{ width: "auto", maxWidth: "80vw" }}
        title={title}
        isOpen={!!message}
      >
        <Body>
          {messageLang ? (
            <HighlightedCodeBlock value={message || ""} languages={["yaml"]} />
          ) : (
            <div style={{ whiteSpace: pre ? "pre-wrap" : "initial" }}>
              {message}
            </div>
          )}
        </Body>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button
              autoFocus={false}
              onClick={(e: React.MouseEvent<any, MouseEvent>) =>
                copyValue(e, message || "")
              }
            >
              Copy
            </Button>
            <Button
              intent="primary"
              autoFocus={true}
              onClick={() => this.setState({ message: undefined })}
            >
              OK
            </Button>
          </div>
        </div>
      </Dialog>
    );
  }
}

const Body = styled.div`
  white-space: pre-line;
  font-family: Consolas, Menlo, monospace;
  font-size: 13px;
  overflow: scroll;
  max-height: 500px;
  background: ${Colors.WHITE};
  border-top: 1px solid ${Colors.LIGHT_GRAY3};
  padding: 20px;
  margin: 0;
  margin-bottom: 20px;
`;
