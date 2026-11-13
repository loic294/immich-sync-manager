import { type ConfirmOptions, IsmConfirmDialog } from '@/components/ism-confirm-dialog';

let dialog: IsmConfirmDialog | undefined;

/** Asks the user to confirm in a modal; resolves `false` on cancel, Escape or backdrop click. */
export function confirmAction(options: ConfirmOptions): Promise<boolean> {
  if (!dialog?.isConnected) {
    dialog = new IsmConfirmDialog();
    document.body.append(dialog);
  }
  return dialog.ask(options);
}
