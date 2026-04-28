tag = v1.1.721


build:
	git commit -am "f" && git push || true
	git tag $(tag)
	git push origin $(tag)


.PHONY: build