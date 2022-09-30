export class EventEmitter {
  subs: Record<string, Array<Function>> = {};
  on(event: string, trigger: Function) {
    if (this.subs[event]) {
      this.subs[event].push(trigger);
    } else {
      this.subs[event] = [trigger];
    }
  }

  remove(event: string, trigger: Function) {
    if (this.subs[event]) {
      this.subs[event] = this.subs[event].filter((x) => x !== trigger);
    }
  }

  removeAll(event: string) {
    delete this.subs[event];
  }

  once(event: string, trigger: Function) {
    const onceTrigger = (...args: any[]): void => {
      trigger(args);
      this.remove(event, onceTrigger);
    };
    this.on(event, onceTrigger);
  }

  emit(event: string, ...args: any[]) {
    if (this.subs[event]) for (const subs of this.subs[event]) subs(...args);
  }
}
