exports.handler = (event, context, callback) => {
    console.log("[CONTEXT] " + JSON.stringify(context));
    console.log("[EVENT] " + JSON.stringify(event));
    callback(null, 'Hello from Lambda');
};
