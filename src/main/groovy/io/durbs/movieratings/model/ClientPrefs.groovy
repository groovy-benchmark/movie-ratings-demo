package io.durbs.movieratings.model

import groovy.transform.CompileStatic

@CompileStatic
class ClientPrefs implements Serializable {

  String theme

  Integer pageSize
}
