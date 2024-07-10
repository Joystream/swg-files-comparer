class Store {
    private static instance: Store;
    private state: { [key: string]: any } = {};

    private constructor() {}

    public static getInstance(): Store {
        if (!Store.instance) {
            Store.instance = new Store();
        }
        return Store.instance;
    }

    public getState(key: string): any {
        return this.state[key];
    }

    public setState(key: string, value: any): void {
        this.state[key] = value;
    }
}

export const store = Store.getInstance();
