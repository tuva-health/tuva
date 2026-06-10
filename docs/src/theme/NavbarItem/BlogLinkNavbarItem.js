import React from 'react';
import clsx from 'clsx';

export default function BlogLinkNavbarItem({
  className,
  isDropdownItem = false,
  label = 'Blog',
  mobile = false,
}) {
  const handleClick = (event) => {
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.altKey ||
      event.ctrlKey ||
      event.metaKey ||
      event.shiftKey
    ) {
      return;
    }

    event.preventDefault();
    window.location.assign('/blog');
  };

  const link = (
    <a
      className={clsx(
        isDropdownItem || mobile ? 'menu__link' : 'navbar__item navbar__link',
        className,
      )}
      href="/blog"
      onClick={handleClick}>
      {label}
    </a>
  );

  if (isDropdownItem) {
    return <li>{link}</li>;
  }

  if (mobile) {
    return <li className="menu__list-item">{link}</li>;
  }

  return link;
}
