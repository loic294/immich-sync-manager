/** Roving focus for the standard horizontal tab lists. */
export function tabKeydown(event: KeyboardEvent) {
  const group = event.currentTarget;
  if (!(group instanceof HTMLElement)) return;
  const tabs = [...group.querySelectorAll<HTMLButtonElement>('[role="tab"]')];
  const current = tabs.indexOf(document.activeElement as HTMLButtonElement);
  let next: number;
  switch (event.key) {
    case 'ArrowRight':
      next = (current + 1) % tabs.length;
      break;
    case 'ArrowLeft':
      next = (current + tabs.length - 1) % tabs.length;
      break;
    case 'Home':
      next = 0;
      break;
    case 'End':
      next = tabs.length - 1;
      break;
    default:
      return;
  }
  event.preventDefault();
  tabs[next]?.focus();
  tabs[next]?.click();
}
