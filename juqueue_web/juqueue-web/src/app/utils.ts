export function filter_obj<V>(
    obj: {[key: string]: V},
    predicate: (key: string, value: V) => boolean
){
    const newObj: {[key: string]: V} = {};

    Object.keys(obj).forEach(
        (key) => {
            if (predicate(key, obj[key]))
                newObj[key] = obj[key]
        }
    );
    return newObj;
}