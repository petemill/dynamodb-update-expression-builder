const emptyListValue = {L: []};
const emptyListKey = ':zz_empty_list';

class DynamoDbUpdateExpressionBuilder {


  constructor({ ShouldValidate = false } = {}){

    this._Adds = [];
    this._Sets = [];
    this._Removes = [];
    this._Deletes = [];
    this._AttributeNames = {};
    this._AttributeValues = {};
    this._LastAttributeValueKey = 'A';
    this._addedEmptyList = false;
    this.ShouldValidate = ShouldValidate;
  }

  _NextValueKey() {

    //increment char, cache it, and return it
    return this._LastAttributeValueKey = nextChar(this._LastAttributeValueKey);
  }


  AddAttributeValue(Value) {

    const attributeValueKey = ':' + this._NextValueKey();
    this._AttributeValues[attributeValueKey] = Value;
    return attributeValueKey;
  }


  AddAttributeNames(AttributeNames) {

    for (const attributeName in AttributeNames) {
      //add actual attribute name to collection of attribute name translations
      this._AttributeNames[attributeName] = AttributeNames[attributeName];
    }
  }

  SetIncrement({ Name, AttributeNames = null, BaseValueIfNotExist = {N: '0'}, IncrementAmount = {N: '1'} }) {

    //collect any optional attribute name references from the 'name' field
    if (AttributeNames) {
      for (const attributeName in AttributeNames) {
        //validate
        if (this.ShouldValidate && !Name.includes(`#${attributeName}`)) {
          throw new Error(`Validation error - Name does not contain key found in AttributeNames '#${attributeName}'`);
        }
        //validation success
        //add actual attribute name to collection of attribute name translations
        this._AttributeNames[attributeName] = AttributeNames[attributeName];
      }
    }
    const baseValueKey = this.AddAttributeValue(BaseValueIfNotExist);
    const incrementValueKey = this.AddAttributeValue(IncrementAmount);
    this._Sets.push(`${Name} = if_not_exists(${Name}, ${baseValueKey}) + ${incrementValueKey}`);
  }

  Set({Name, SetExpression, IfNotExists = false, AttributeNames = null, Increment = false, Value, ListAppend = null}) {

    //collect any optional attribute name references from the 'name' field
    if (AttributeNames) {
      for (const attributeName in AttributeNames) {
        //validate
        if (this.ShouldValidate && !Name.includes(`#${attributeName}`)) {
          throw new Error(`Validation error - Name does not contain key found in AttributeNames '#${attributeName}'`);
        }
        //validation success
        //add actual attribute name to collection of attribute name translations
        this._AttributeNames[attributeName] = AttributeNames[attributeName];
      }
    }
    //custom/manual complex statement
    if (SetExpression) {
      if (Value || Name)
        throw new Error('You cannot provide both a Name or Value and a ManualSetFn. Use the AddAttributeValue instance function to add one or more Values, and get the keys for them back, first.');
      this._Sets.push(SetExpression);
    }
    //push the assignment expression, adjusting for any supported expressions
    else if (!ListAppend) {
      const attributeValueKey = ':' + this._NextValueKey();
      this._AttributeValues[attributeValueKey] = Value;
      if (IfNotExists) {
        this._Sets.push(`${Name} = if_not_exists(${Name}, ${attributeValueKey})`);
      }
      else {
        this._Sets.push(`${Name} = ${attributeValueKey}`);
      }
    }
    //otherwise we are prepending to or appending to a list
    else {
      //support creating new lists if the property doesn't yet exist on the item
      if (!this._addedEmptyList) {
        this._AttributeValues[emptyListKey] = emptyListValue;
        this._addedEmptyList = true;
      }
      //add the value to our list, always an array
      const attributeValueKey = ':' + this._NextValueKey();
      this._AttributeValues[attributeValueKey] = {L: Value};
      //work out ordering of assignment based on append or prepend
      const namePart = `if_not_exists(${Name}, ${emptyListKey})`;
      let firstPart;
      let secondPart;
      if (ListAppend === 'start') {
        firstPart = attributeValueKey;
        secondPart = namePart;
      }
      else { //assume 'end'
        firstPart = namePart;
        secondPart = attributeValueKey;
      }
      //push the list assignment statement
      this._Sets.push(`${Name} = list_append(${firstPart}, ${secondPart})`);
    }
  }


  Remove({Name, AttributeNames}) {

    //collect any optional attribute name references from the 'name' field
    if (AttributeNames) {
      for (const attributeName in AttributeNames) {
        //validate
        if (this.ShouldValidate && !Name.includes(`#${attributeName}`)) {
          throw new Error(`Validation error - Name does not contain key found in AttributeNames '#${attributeName}'`);
        }
        //validation success
        //add actual attribute name to collection of attribute name translations
        this._AttributeNames[attributeName] = AttributeNames[attributeName];
      }
    }
    //push the assignment expression
    this._Removes.push(`${Name}`);
  }


  Add({Name, AttributeNames = null, Value}) {

    //collect any optional attribute name references from the 'name' field
    if (AttributeNames) {
      for (const attributeName in AttributeNames) {
        //validate
        if (this.ShouldValidate && !Name.includes(`#${attributeName}`)) {
          throw new Error(`Validation error - Name does not contain key found in AttributeNames '#${attributeName}'`);
        }
        //validation success
        //add actual attribute name to collection of attribute name translations
        this._AttributeNames[attributeName] = AttributeNames[attributeName];
      }
    }
    //assign an attribute value key
    const attributeValueKey = ':' + this._NextValueKey();
    //push the assignment expression
    this._Adds.push(`${Name} ${attributeValueKey}`);
    //store the value the expression assignment refers to
    this._AttributeValues[attributeValueKey] = Value;
  }


  Delete({Name, AttributeNames = null, Value}) {

    //collect any optional attribute name references from the 'name' field
    if (AttributeNames) {
      for (const attributeName in AttributeNames) {
        //validate
        if (this.ShouldValidate && !Name.includes(`#${attributeName}`)) {
          throw new Error(`Validation error - Name does not contain key found in AttributeNames '#${attributeName}'`);
        }
        //validation success
        //add actual attribute name to collection of attribute name translations
        this._AttributeNames[attributeName] = AttributeNames[attributeName];
      }
    }
    //assign an attribute value key
    const attributeValueKey = ':' + this._NextValueKey();
    //push the assignment expression
    this._Deletes.push(`${Name} ${attributeValueKey}`);
    //store the value the expression assignment refers to
    this._AttributeValues[attributeValueKey] = Value;
  }


  UpdateExpressionParams(){

    const params = {
      //update expression is a concatenated string
      UpdateExpression: ExpressionSection('SET', this._Sets) +
        ExpressionSection(' REMOVE', this._Removes) +
        ExpressionSection(' ADD', this._Adds) +
        ExpressionSection(' DELETE', this._Deletes),
    };
    //if no expression, just return empty object
    if (!params.UpdateExpression) {
      return {};
    }
    //only return attribute names param if there are any
    if (Object.keys(this._AttributeNames).length)
      params.ExpressionAttributeNames = this._AttributeNames;
    //only return attribute values param if there are any
    if (Object.keys(this._AttributeValues).length)
      params.ExpressionAttributeValues = this._AttributeValues;
    //params object is ready for ddb AWS API
    return params;
  }


}



module.exports = DynamoDbUpdateExpressionBuilder;


function ExpressionSection(sectionName, sectionParams) {

  if (sectionParams.length) {
    return sectionName + ' ' + sectionParams.join(', ');
  }
  return '';
}

//
// nextChar() provided by http://stackoverflow.com/a/34483399/663320
//
function nextChar(c) {

    var u = c.toUpperCase();
    if (same(u,'Z')){
        var txt = '';
        var i = u.length;
        while (i--) {
            txt += 'A';
        }
        return (txt+'A');
    } else {
        var p = "";
        var q = "";
        if(u.length > 1){
            p = u.substring(0, u.length - 1);
            q = String.fromCharCode(p.slice(-1).charCodeAt(0));
        }
        var l = u.slice(-1).charCodeAt(0);
        var z = nextLetter(l);
        if(z==='A'){
            return p.slice(0,-1) + nextLetter(q.slice(-1).charCodeAt(0)) + z;
        } else {
            return p + z;
        }
    }
}

function nextLetter(l){

    if(l<90){
        return String.fromCharCode(l + 1);
    }
    else{
        return 'A';
    }
}

function same(str,char){

    var i = str.length;
    while (i--) {
        if (str[i]!==char){
            return false;
        }
    }
    return true;
}
